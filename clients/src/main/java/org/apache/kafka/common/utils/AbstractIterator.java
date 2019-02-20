/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A base class that simplifies implementing an iterator
 * @param <T> The type of thing we are iterating over
 */
public abstract class AbstractIterator<T> implements Iterator<T> {

    private static enum State {
        READY, NOT_READY, DONE, FAILED
    };

    private State state = State.NOT_READY;
    private T next;

    @Override
    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("Iterator is in failed state");
            case DONE://迭代结束，返回false.
                return false;
            case READY://迭代准备好了，返回true
                return true;
            default://NOT_READY状态，需要调用maybeComputeNext()方法获取next项
                return maybeComputeNext();
        }
    }

    @Override
    public T next() {
        if (!hasNext())
            throw new NoSuchElementException();
        state = State.NOT_READY;//将state字段重置为NOT_READY状态
        if (next == null)
            throw new IllegalStateException("Expected item but none found.");
        return next;//返回next
    }
    //不支持remove操作，调用报异常。
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    public T peek() {
        if (!hasNext())
            throw new NoSuchElementException();
        return next;
    }
    //子类在实现makeNext()方法中可以通过调用allDone()方法结束整个迭代
    protected T allDone() {
        state = State.DONE;
        return null;
    }
    //子类实现，返回下一个迭代项
    protected abstract T makeNext();

    private Boolean maybeComputeNext() {
        state = State.FAILED;//若在子类的实现makeNext()中抛出异常，则state会处于这个状态
        next = makeNext();
        if (state == State.DONE) {
            return false;
        } else {//在makeNext()方法中完成了next项的构造
            state = State.READY;
            return true;
        }
    }

}
