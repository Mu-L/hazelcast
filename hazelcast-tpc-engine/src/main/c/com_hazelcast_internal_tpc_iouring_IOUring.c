/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <liburing.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "include/com_hazelcast_internal_tpc_iouring_IOUring.h"
#include "include/io_uring.h"
#include "include/utils.h"

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_init(JNIEnv* env, jobject this_object,
                                                     jint entries, jint flags){
    struct io_uring *p_ring = malloc(sizeof(struct io_uring));

    if (!p_ring) {
        throw_out_of_memory_error(env);
        return;
    }

    int ret = io_uring_queue_init(entries, p_ring, flags);
    if (ret < 0) {
        throw_exception(env, "io_uring_queue_init", ret);
        return;
    }

    jclass this_class = (*env)->GetObjectClass(env, this_object);

    jfieldID ringAddr_FieldId = (*env)->GetFieldID(env, this_class, "ringAddr","J");
    (*env)->SetLongField(env, this_object, ringAddr_FieldId, (jlong) p_ring);

    jfieldID ringFd_FieldId = (*env)->GetFieldID(env, this_class, "ringFd","I");
    (*env)->SetIntField(env, this_object, ringFd_FieldId, (jint) p_ring->ring_fd);
}


JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_enter(JNIEnv* env, jclass this_class, jint fd,
                                                      jint to_submit, jint min_complete, jint flags){
    int res = io_uring_enter(fd, to_submit, min_complete, flags, NULL);
    if (res < 0){
        return throw_exception(env, "io_uring_enter", res);
    }
    return res;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_exit(JNIEnv* env, jclass this_class, jlong ring){
    io_uring_queue_exit((struct io_uring *)ring);
}
