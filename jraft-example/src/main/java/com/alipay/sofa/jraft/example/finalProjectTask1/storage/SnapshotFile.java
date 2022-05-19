/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.example.finalProjectTask1.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SnapshotFile {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotFile.class);

    private String              path;

    public SnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }


    public boolean save(final Map<String, Integer> accounts) {
        try {
            LOG.info("Saving SnapShot to path : " + path);

            FileOutputStream fos = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            oos.writeObject(accounts);

            oos.close();
            fos.close();

            return true;
        }
        catch (IOException e) {
            LOG.error("Fail to save snapshot", e);
            return false;
        }
    }

    public Map<String, Integer> load() throws IOException, ClassNotFoundException {
        LOG.info("Loading SnapShot from path : " + path);
        ConcurrentHashMap<String, Integer> accounts = null;

        FileInputStream fis = new FileInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(fis);

        accounts = (ConcurrentHashMap<String, Integer>) ois.readObject();

        ois.close();
        fis.close();

        return accounts;
    }
}
