int xnet_resend(struct xnet_context *xc, struct xnet_msg *msg)
{
    struct epoll_event ev;
    struct xnet_msg *pos, *n;
    struct xnet_site *xs;
    struct xnet_addr *xa;
    int err = 0, csock = 0, found = 0, reconn = 0;
    int ssock = 0;              /* selected socket */
    int nr_conn = 0;
    int lock_idx = 0;
    int __attribute__((unused))bw, bt, msg_found = 0;

    if (unlikely(!xc)) {
        hvfs_err(xnet, "XNET has not been inited yet.\n");
        return -EINVAL;
    }

    xlock_lock(&xc->resend_lock);
    list_for_each_entry_safe(pos, n, &xc->resend_q, list) {
        if (msg == pos) {
            list_del_init(&pos->list);
            msg_found = 1;
        }
        break;
    }
    if (msg_found) {
        /* add to the resend queue */
        msg->ts = time(NULL);
        list_add_tail(&pos->list, &xc->resend_q);
    }
    xlock_unlock(&xc->resend_lock);
    if (!msg_found)
        return 0;

    if (msg->tx.ssite_id == msg->tx.dsite_id) {
        hvfs_err(xnet, "Warning: target site is the original site, BYPASS?\n");
    }

    if (msg->state == XNET_MSG_ACKED || 
        msg->state == XNET_MSG_COMMITED ||
        atomic_read(&msg->ref) == 1) {
        /* remove from the resend Q */
        xlock_lock(&xc->resend_lock);
        list_del_init(&msg->list);
        xlock_unlock(&xc->resend_lock);
        if (atomic_dec_return(&msg->ref) == 0) {
            xnet_free_msg(msg);
        }
        return 0;
    }

    err = st_lookup(&gst, &xs, msg->tx.dsite_id);
    if (err) {
        hvfs_err(xnet, "Dest Site(%lx) unreachable.\n", msg->tx.dsite_id);
        return -EINVAL;
    }
retry:
    err = 0;
    list_for_each_entry(xa, &xs->addr, list) {
        if (!IS_CONNECTED(xa, xc)) {
            /* not connected, dynamic connect */
            /* Note that: we should serialize the connecting threads here! */
            xlock_lock(&xa->clock);
            if (!csock) {
                csock = socket(AF_INET, SOCK_STREAM, 0);
                if (csock < 0) {
                    hvfs_err(xnet, "socket() failed %d\n", errno);
                    err = -errno;
                    goto out;
                }
            }
#ifdef XNET_CONN_EINTR
            do {
                err = connect(csock, &xa->sa, sizeof(xa->sa));
                if (err < 0) {
                    if (errno == EISCONN) {
                        err = 0;
                        break;
                    }
                    if (errno == EINTR)
                        continue;
                    else
                        break;
                } else
                    break;
            } while (1);
#else
            err = connect(csock, &xa->sa, sizeof(xa->sa));
#endif
            
            if (err < 0) {
                xlock_unlock(&xa->clock);
                hvfs_err(xnet, "connect() %s %d failed '%s' %d times\n",
                         inet_ntoa(((struct sockaddr_in *)&xa->sa)->sin_addr),
                         ntohs(((struct sockaddr_in *)&xa->sa)->sin_port), 
                         strerror(errno), reconn);
                err = -errno;
                if (reconn < 10) {
                    reconn++;
                    sleep(1);
                    goto retry;
                }
                close(csock);
                goto out;
            } else {
                struct active_conn *ac;

            realloc:                
                ac = xzalloc(sizeof(struct active_conn));
                if (!ac) {
                    hvfs_err(xnet, "xzalloc() struct active_conn failed\n");
                    sleep(1);
                    goto realloc;
                }
                INIT_LIST_HEAD(&ac->list);
                ac->sockfd = csock;
                xlock_lock(&active_list_lock);
                list_add_tail(&ac->list, &active_list);
                xlock_unlock(&active_list_lock);

                /* yap, we have connected, following the current protocol, we
                 * should send a hello msg and recv a hello ack msg */
                {
                    struct xnet_msg_tx htx = {
                        .version = 0,
                        .len = sizeof(htx),
                        .type = XNET_MSG_HELLO,
                        .ssite_id = xc->site_id,
                        .dsite_id = msg->tx.dsite_id,
                    };
                    struct iovec __iov = {
                        .iov_base = &htx,
                        .iov_len = sizeof(htx),
                    };
                    struct msghdr __msg = {
                        .msg_iov = &__iov,
                        .msg_iovlen = 1,
                    };
                    int bt, br;

                    bt = sendmsg(csock, &__msg, MSG_NOSIGNAL);
                    if (bt < 0 || bt < sizeof(htx)) {
                        xlock_unlock(&xa->clock);
                        hvfs_err(xnet, "sendmsg do not support redo now(%s) "
                                 ":(\n", strerror(errno));
                        err = -errno;
                        close(csock);
                        csock = 0;
                        goto retry;
                    }
                    /* recv the hello ack in handle_tx thread */
                    br = 0;
                    do {
                        bt = recv(csock, (void *)&htx + br, sizeof(htx) - br,
                                  MSG_WAITALL | MSG_NOSIGNAL);
                        if (bt < 0) {
                            if (errno == EAGAIN || errno == EINTR) {
                                sched_yield();
                                continue;
                            }
                            xlock_unlock(&xa->clock);
                            hvfs_err(xnet, "recv error: %s\n", strerror(errno));
                            close(csock);
                            csock = 0;
                            goto retry;
                        } else if (bt == 0) {
                            xlock_unlock(&xa->clock);
                            close(csock);
                            csock = 0;
                            goto retry;
                        }
                        br += bt;
                    } while (br < sizeof(htx));
                }

                /* now, it is ok to push this socket to the epoll thread */
                setnonblocking(csock);
                setnodelay(csock);
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = csock;
                err = epoll_ctl(epfd, EPOLL_CTL_ADD, csock, &ev);
                if (err < 0) {
                    xlock_unlock(&xa->clock);
                    hvfs_err(xnet, "epoll_ctl() add fd %d to SET(%d) "
                             "failed %d\n", 
                             csock, epfd, errno);
                    close(csock);
                    csock = 0;
                    sleep(1);
                    goto retry;
                }
                
                /* ok, it is ok to update this socket to the site table */
                err = st_update_sockfd(&gst, csock, msg->tx.dsite_id);
                if (err) {
                    xlock_unlock(&xa->clock);
                    /* do NOT remove from the epoll set, for pollin thread to
                     * tear down the connection */
                    st_clean_sockfd(&gst, csock);
                    csock = 0;
                    sleep(1);
                    goto retry;
                }

                hvfs_debug(xnet, "We just create connection %s %d -> fd %d\n",
                           inet_ntoa(((struct sockaddr_in *)&xa->sa)->sin_addr),
                           ntohs(((struct sockaddr_in *)&xa->sa)->sin_port), 
                           csock);

                nr_conn++;
#if 0
                if (nr_conn < XNET_CONNS_DEF) {
                    csock = 0;
                    xlock_unlock(&xa->clock);
                    goto retry;
                }
#endif
            }
            found = 1;
            xlock_unlock(&xa->clock);
            break;
        } else {
            found = 1;
            if (csock)
                close(csock);
            break;
        }
    }

    if (!found) {
        hvfs_err(xnet, "Sorry, we can not find the target site %ld\n",
                 msg->tx.dsite_id);
        err = -EINVAL;
        goto out;
    }
    
    msg->tx.ssite_id = xc->site_id;
    /* the reqno is set, and it has also added to rpy cache */
    if (msg->tx.type != XNET_MSG_RPY)
        msg->tx.handle = (u64)msg;
    if (g_xnet_conf.use_rpy_cache)
        xrc_set_cached(msg);

reselect_conn:
    ssock = SELECT_CONNECTION(xa, &lock_idx);
    /* already connected, just send the message */
    hvfs_debug(xnet, "OK, select connection %d, we will send the msg "
               "site %lx -> %lx ...\n", ssock, 
               msg->tx.ssite_id, msg->tx.dsite_id);
    if (ssock < 0) {
        err = -EINVAL;
        goto out;
    }
    
#ifndef XNET_EAGER_WRITEV
    /* send the msg tx by the selected connection */
    bw = 0;
    do {
        bt = send(ssock, ((void *)&msg->tx) + bw, 
                  sizeof(struct xnet_msg_tx) - bw, MSG_MORE);
        if (bt < 0) {
            hvfs_err(xnet, "write() err %d\n", errno);
            if (errno == EINTR || errno == EAGAIN)
                continue;
            err = -errno;
            goto out_unlock;
        }
        bw += bt;
    } while (bw < sizeof(struct xnet_msg_tx));
    atomic64_add(bw, &g_xnet_prof.outbytes);
#endif

    /* then, send the data region */
    if (msg->siov_ulen) {
        hvfs_debug(xnet, "There is some data to send (iov_len %d) len %d.\n",
                   msg->siov_ulen, msg->tx.len);
#ifdef XNET_BLOCKING
        {
            struct msghdr __msg = {
                .msg_iov = msg->siov,
                .msg_iovlen = msg->siov_ulen,
            };

            bw = 0;
            do {
                bt = sendmsg(ssock, &__msg, MSG_NOSIGNAL);
                if (bt < 0 || msg->tx.len > bt) {
                    if (bt >= 0) {
                        /* ok, we should adjust the iov */
                        __iov_recal(msg->siov, &__msg.msg_iov, msg->siov_ulen,
                                    &__msg.msg_iovlen, bt + bw);
                        bw += bt;
                        continue;
                    } else if (errno == EINTR || errno == EAGAIN) {
                        continue;
                    }
                    hvfs_err(xnet, "sendmsg(%d[%lx],%d,%d) err %d, "
                             "for now we do support redo:)\n", 
                             ssock, msg->tx.dsite_id, bt,
                             msg->tx.len, 
                             errno);
                    if (errno == ECONNRESET || errno == EBADF || 
                        errno == EPIPE) {
                        /* select another link and resend the whole msg */
                        xlock_unlock(&xa->socklock[lock_idx]);
                        st_clean_sockfd(&gst, ssock);
                        hvfs_err(xnet, "Reselect Conn [%d] --> ?\n", ssock);
                        goto reselect_conn;
                    }
                    err = -errno;
                    goto out_unlock;
                }
                bw += bt;
            } while (bw < msg->tx.len);
            atomic64_add(bt, &g_xnet_prof.outbytes);
        }
#elif 1
        bt = writev(ssock, msg->siov, msg->siov_ulen);
        if (bt < 0 || msg->tx.len > bt) {
            hvfs_err(xnet, "writev(%d[%lx]) err %d, for now we do not "
                     "support redo:(\n", ssock, msg->tx.dsite_id,
                     errno);
            err = -errno;
            goto out_unlock;
        }
        atomic64_add(bt, &g_xnet_prof.outbytes);
#else
        int i;

        for (i = 0; i < msg->siov_ulen; i++) {
            bw = 0;
            do {
                bt = write(ssock, msg->siov[i].iov_base + bw, 
                           msg->siov[i].iov_len - bw);
                if (bt < 0) {
                    hvfs_err(xnet, "write() err %d\n", errno);
                    if (errno == EINTR || errno == EAGAIN)
                        continue;
                    err = -errno;
                    goto out_unlock;
                }
                bw += bt;
            } while (bw < msg->siov[i].iov_len);
            atomic64_add(bw, &g_xnet_prof.outbytes);
        }
#endif
    }

    xlock_unlock(&xa->socklock[lock_idx]);

    hvfs_debug(xnet, "We have sent the msg %p throuth link %d\n", msg, ssock);

out:
    return err;
out_unlock:
    xlock_unlock(&xa->socklock[lock_idx]);
    return err;
}

/* xnet_send()
 */
int xnet_send(struct xnet_context *xc, struct xnet_msg *msg)
{
    struct epoll_event ev;
    struct xnet_site *xs;
    struct xnet_addr *xa;
    int err = 0, csock = 0, found = 0, reconn = 0;
    int ssock = 0;              /* selected socket */
    int nr_conn = 0;
    int lock_idx = 0;
    u32 bw;
    int bt;

    if (unlikely(!xc)) {
        hvfs_err(xnet, "XNET has not been inited yet.\n");
        return -EINVAL;
    }
    
    if (unlikely(msg->tx.ssite_id == msg->tx.dsite_id)) {
        hvfs_err(xnet, "Warning: target site is the original site, BYPASS?\n");
    }

    /* Setup our magic now */
    msg->tx.magic = g_xnet_conf.magic;
    
    if (unlikely(g_xnet_conf.enable_resend)) {
        if (msg->tx.flag & XNET_NEED_RESEND ||
            msg->tx.flag & XNET_NEED_REPLY) {
            /* add to the resend queue */
            msg->ts = time(NULL);
            msg->xc = xc;
            xlock_lock(&xc->resend_lock);
            list_add_tail(&msg->list, &xc->resend_q);
            atomic_inc(&msg->ref);
            xlock_unlock(&xc->resend_lock);
        }
    }

    err = st_lookup(&gst, &xs, msg->tx.dsite_id);
    if (unlikely(err)) {
        hvfs_err(xnet, "Dest Site(%lx) unreachable.\n", msg->tx.dsite_id);
        return -EINVAL;
    }
retry:
    err = 0;
    list_for_each_entry(xa, &xs->addr, list) {
        if (unlikely(!IS_CONNECTED(xa, xc))) {
            /* not connected, dynamic connect */
            xlock_lock(&xa->clock);
            if (!csock) {
                csock = socket(AF_INET, SOCK_STREAM, 0);
                if (csock < 0) {
                    hvfs_err(xnet, "socket() failed %d\n", errno);
                    err = -errno;
                    xlock_unlock(&xa->clock);
                    goto out;
                }
            }
#ifdef XNET_CONN_EINTR
            do {
                err = connect(csock, &xa->sa, sizeof(xa->sa));
                if (err < 0) {
                    if (errno == EISCONN) {
                        err = 0;
                        break;
                    }
                    if (errno == EINTR)
                        continue;
                    else
                        break;
                } else
                    break;
            } while (1);
#else
            err = connect(csock, &xa->sa, sizeof(xa->sa));
#endif
            if (err < 0) {
                xlock_unlock(&xa->clock);
                hvfs_err(xnet, "connect() %s %d failed '%s' %d times\n",
                         inet_ntoa(((struct sockaddr_in *)&xa->sa)->sin_addr),
                         ntohs(((struct sockaddr_in *)&xa->sa)->sin_port), 
                         strerror(errno), reconn);
                err = -errno;
                if (reconn < 10) {
                    reconn++;
                    sleep(1);
                    goto retry;
                }
                close(csock);
                goto out;
            } else {
                struct active_conn *ac;

            realloc:                
                ac = xzalloc(sizeof(struct active_conn));
                if (!ac) {
                    hvfs_err(xnet, "xzalloc() struct active_conn failed\n");
                    sleep(1);
                    goto realloc;
                }
                INIT_LIST_HEAD(&ac->list);
                ac->sockfd = csock;
                xlock_lock(&active_list_lock);
                list_add_tail(&ac->list, &active_list);
                xlock_unlock(&active_list_lock);

                /* yap, we have connected, following the current protocol, we
                 * should send a hello msg and recv a hello ack msg */
                {
                    struct xnet_msg_tx htx = {
                        .version = 0,
                        .len = sizeof(htx),
                        .type = XNET_MSG_HELLO,
                        .ssite_id = xc->site_id,
                        .dsite_id = msg->tx.dsite_id,
                    };
                    struct iovec __iov = {
                        .iov_base = &htx,
                        .iov_len = sizeof(htx),
                    };
                    struct msghdr __msg = {
                        .msg_iov = &__iov,
                        .msg_iovlen = 1,
                    };
                    int bt, br;

                    bt = sendmsg(csock, &__msg, MSG_NOSIGNAL);
                    if (bt < 0 || bt < sizeof(htx)) {
                        xlock_unlock(&xa->clock);
                        hvfs_err(xnet, "sendmsg do not support redo now(%s) "
                                 ":(\n", strerror(errno));
                        err = -errno;
                        close(csock);
                        csock = 0;
                        goto retry;
                    }
                    /* recv the hello ack in handle_tx thread */
                    br = 0;
                    do {
                        bt = recv(csock, (void *)&htx + br, sizeof(htx) - br,
                                  MSG_WAITALL | MSG_NOSIGNAL);
                        if (bt < 0) {
                            if (errno == EAGAIN || errno == EINTR) {
                                sched_yield();
                                continue;
                            }
                            xlock_unlock(&xa->clock);
                            hvfs_err(xnet, "recv error: %s\n", strerror(errno));
                            close(csock);
                            csock = 0;
                            goto retry;
                        } else if (bt == 0) {
                            xlock_unlock(&xa->clock);
                            close(csock);
                            csock = 0;
                            goto retry;
                        }
                        br += bt;
                    } while (br < sizeof(htx));
                }

                /* now, it is ok to push this socket to the epoll thread */
                setnonblocking(csock);
                setnodelay(csock);
                ev.events = EPOLLIN | EPOLLET;
                ev.data.fd = csock;
                err = epoll_ctl(epfd, EPOLL_CTL_ADD, csock, &ev);
                if (err < 0) {
                    xlock_unlock(&xa->clock);
                    hvfs_err(xnet, "epoll_ctl() add fd %d to SET(%d) "
                             "failed %d\n", 
                             csock, epfd, errno);
                    close(csock);
                    csock = 0;
                    sleep(1);
                    goto retry;
                }
                
                /* ok, it is ok to update this socket to the site table */
                err = st_update_sockfd(&gst, csock, msg->tx.dsite_id);
                if (err) {
                    xlock_unlock(&xa->clock);
                    /* do NOT remove from the epoll set, for pollin thread to
                     * tear down the connection */
                    st_clean_sockfd(&gst, csock);
                    csock = 0;
                    sleep(1);
                    goto retry;
                }

                hvfs_debug(xnet, "We just create connection %s %d -> fd %d\n",
                           inet_ntoa(((struct sockaddr_in *)&xa->sa)->sin_addr),
                           ntohs(((struct sockaddr_in *)&xa->sa)->sin_port), 
                           csock);

                nr_conn++;
#if 0                           /* Note that: the following section is used to
                                 * generate heavy connect load */
                if (nr_conn < XNET_CONNS_DEF) {
                    csock = 0;
                    xlock_unlock(&xa->clock);
                    goto retry;
                }
#endif
            }
            found = 1;
            xlock_unlock(&xa->clock);
            break;
        } else {
            found = 1;
            if (csock)
                close(csock);
            break;
        }
    }

    if (unlikely(!found)) {
        hvfs_err(xnet, "Sorry, we can not find the target site %ld\n",
                 msg->tx.dsite_id);
        err = -EINVAL;
        goto out;
    }
    
    msg->tx.ssite_id = xc->site_id;
    if (msg->tx.type != XNET_MSG_RPY)
        msg->tx.handle = (u64)msg;
    if (msg->tx.type == XNET_MSG_REQ) {
        msg->tx.reqno = atomic_inc_return(&global_reqno);
        /* add to the rpy cache if needed */
        if (g_xnet_conf.use_rpy_cache) {
            err = rpy_cache_add(msg);
            if (err) {
                hvfs_err(xnet, "Add to rpy cache failed, abort sending\n");
                goto out;
            }
            xrc_set_cached(msg);
        }
    }

reselect_conn:
    ssock = SELECT_CONNECTION(xa, &lock_idx);
    /* already connected, just send the message */
    hvfs_debug(xnet, "OK, select connection %d, we will send the msg "
               "site %lx -> %lx ...\n", ssock, 
               msg->tx.ssite_id, msg->tx.dsite_id);
    if (unlikely(ssock < 0)) {
        err = -EINVAL;
        goto out;
    }
    
#ifndef XNET_EAGER_WRITEV
    /* send the msg tx by the selected connection */
    bw = 0;
    do {
        bt = send(ssock, ((void *)&msg->tx) + bw, 
                  sizeof(struct xnet_msg_tx) - bw, MSG_MORE);
        if (bt < 0) {
            hvfs_err(xnet, "write() err %d\n", errno);
            if (errno == EINTR || errno == EAGAIN)
                continue;
            err = -errno;
            goto out_unlock;
        }
        bw += bt;
    } while (bw < sizeof(struct xnet_msg_tx));
    atomic64_add(bw, &g_xnet_prof.outbytes);
#endif

    /* then, send the data region */
    if (msg->siov_ulen) {
        hvfs_debug(xnet, "There is some data to send (iov_len %d) len %d.\n",
                   msg->siov_ulen, msg->tx.len);
#ifdef XNET_BLOCKING
        {
            struct msghdr __msg = {
                .msg_iov = msg->siov,
                .msg_iovlen = msg->siov_ulen,
            };

            if (msg->tx.len <= __MAX_MSG_SIZE) {
                bw = 0;
                do {
                    bt = sendmsg(ssock, &__msg, MSG_NOSIGNAL);
                    if (bt > 0) {
                            /* ok, we should adjust the iov. */
                        if (msg->siov != __msg.msg_iov)
                            xfree(__msg.msg_iov);
                        __iov_recal(msg->siov, &__msg.msg_iov, 
                                    msg->siov_ulen,
                                    &__msg.msg_iovlen, bt + bw);
                        bw += bt;
                        continue;
                    } else if (bt == 0) {
                        hvfs_warning(xnet, "Zero sent at offset "
                                     "%u -> %u\n",
                                     bw, msg->tx.len);
                        continue;
                    } else if (bt < 0) {
                        /* this means bt < 0, an error occurs */
                        if (errno == EINTR || errno == EAGAIN) {
                            continue;
                        }
                        hvfs_err(xnet, "sendmsg(%d[%lx],%d,%x,flag %x) "
                                 "err %d, for now we do support redo:)\n", 
                                 ssock, msg->tx.dsite_id, bt,
                                 msg->tx.len, msg->tx.flag, 
                                 errno);
                        if (errno == ECONNRESET || errno == EBADF || 
                            errno == EPIPE) {
                            /* select another link and resend the whole msg */
                            xlock_unlock(&xa->socklock[lock_idx]);
                            st_clean_sockfd(&gst, ssock);
                            hvfs_err(xnet, "Reselect Conn [%d] --> ?\n", 
                                     ssock);
                            goto reselect_conn;
                        }
                        err = -errno;
                        goto out_unlock;
                    }
                    bw += bt;
                } while (bw < msg->tx.len);
                atomic64_add(bw, &g_xnet_prof.outbytes);
                if (msg->siov != __msg.msg_iov)
                    xfree(__msg.msg_iov);
            } else {
                /* this means we have trouble. user give us a larger message,
                 * and we have to split it into smaller slices. make sure we
                 * send the whole message in ONE connection */
                u64 send_offset = 0, this_len = 0;
                
                do {
                    this_len = send_offset;
                    __iov_cut(msg->siov, &__msg.msg_iov,
                              msg->siov_ulen, &__msg.msg_iovlen,
                              &send_offset);
                    this_len = send_offset - this_len;
                    /* do actually send here */
                    {
                        struct msghdr __msg2 = __msg;

                        bw = 0;
                        do {
                            bt = sendmsg(ssock, &__msg, MSG_NOSIGNAL);
                            if (bt > 0) {
                                /* note that we have to adjust the iov
                                 * carefully */
                                if (__msg2.msg_iov != __msg.msg_iov)
                                    xfree(__msg.msg_iov);
                                __iov_recal(__msg2.msg_iov, &__msg.msg_iov, 
                                            __msg2.msg_iovlen,
                                            &__msg.msg_iovlen, bt + bw);
                                bw += bt;
                                continue;
                            } else if (bt == 0) {
                                hvfs_warning(xnet, "Zero sent at offset "
                                             "%u -> %lu\n", 
                                             bw, this_len);
                                continue;
                            } else {
                                /* this means bt < 0, an error occurs */
                                if (errno == EINTR || errno == EAGAIN) {
                                    continue;
                                }
                                hvfs_err(xnet, "sendmsg(%d[%lx],%d,%lx from"
                                         " %lx,flag %x) err %d, for now we "
                                         "do support redo:)\n", 
                                         ssock, msg->tx.dsite_id, bt,
                                         this_len, send_offset - this_len, 
                                         msg->tx.flag, errno);
                                if (errno == ECONNRESET || errno == EBADF || 
                                    errno == EPIPE) {
                                    /* select another link and resend the
                                     * whole msg */
                                    xlock_unlock(&xa->socklock[lock_idx]);
                                    st_clean_sockfd(&gst, ssock);
                                    xfree(__msg.msg_iov);
                                    hvfs_err(xnet, "Reselect Conn [%d] --> ?\n", 
                                             ssock);
                                    goto reselect_conn;
                                }
                                err = -errno;
                                goto out_unlock;
                            }
                            bw += bt;
                        } while (bw < this_len);
                        atomic64_add(bw, &g_xnet_prof.outbytes);
                    }
                    xfree(__msg.msg_iov);
                } while (send_offset < msg->tx.len);
            }
        }
#elif 1
        bt = writev(ssock, msg->siov, msg->siov_ulen);
        if (bt < 0 || msg->tx.len > bt) {
            hvfs_err(xnet, "writev(%d[%lx]) err %d, for now we do not "
                     "support redo:(\n", ssock, msg->tx.dsite_id,
                     errno);
            err = -errno;
            goto out_unlock;
        }
        atomic64_add(bt, &g_xnet_prof.outbytes);
#else
        int i;

        for (i = 0; i < msg->siov_ulen; i++) {
            bw = 0;
            do {
                bt = write(ssock, msg->siov[i].iov_base + bw, 
                           msg->siov[i].iov_len - bw);
                if (bt < 0) {
                    hvfs_err(xnet, "write() err %d\n", errno);
                    if (errno == EINTR || errno == EAGAIN)
                        continue;
                    err = -errno;
                    goto out_unlock;
                }
                bw += bt;
            } while (bw < msg->siov[i].iov_len);
            atomic64_add(bw, &g_xnet_prof.outbytes);
        }
#endif
    }

    xlock_unlock(&xa->socklock[lock_idx]);

    hvfs_debug(xnet, "We have sent the msg %p throuth link %d\n", msg, ssock);

    /* finally, we wait for the reply msg */
    if (msg->tx.flag & XNET_NEED_REPLY) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += g_xnet_conf.send_timeout;
        /* adding the length judgement here, refer to 64MB/s */
        ts.tv_sec += msg->tx.len >> 26;
    rewait:
        err = sem_timedwait(&msg->event, &ts);
        if (err < 0) {
            if (errno == EINTR)
                goto rewait;
            else if (errno == ETIMEDOUT) {
                hvfs_err(xnet, "Send to %lx time out for %d seconds.\n",
                         msg->tx.dsite_id, g_xnet_conf.send_timeout);
                err = -ETIMEDOUT;
                /* we need to remove the msg from rpy cache if needed */
                if (g_xnet_conf.use_rpy_cache) {
                    rpy_cache_find_del(msg);
                }
            } else
                hvfs_err(xnet, "sem_wait() failed %d\n", errno);
        }

        /* Haaaaa, we got the reply now */
        hvfs_debug(xnet, "We(%p) got the reply msg %p.\n", msg, msg->pair);
    }
    
out:
    return err;
out_unlock:
    xlock_unlock(&xa->socklock[lock_idx]);
    return err;
}
