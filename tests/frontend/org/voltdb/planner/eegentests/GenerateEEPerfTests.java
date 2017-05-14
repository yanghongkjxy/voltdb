/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package org.voltdb.planner.eegentests;

import org.voltdb.catalog.Database;

public class GenerateEEPerfTests extends EEPlanGenerator {
    private static final String DDL_FILENAME = "testplans-ee-perfgenerators.sql";

    @Override
    public void setUp() throws Exception {
        super.setUp(GenerateEETests.class.getResource(DDL_FILENAME),
                    "testplansperfgenerator",
                    true);
    }

    public static void main(String args[]) {
        GenerateEEPerfTests tg = new GenerateEEPerfTests();
        tg.processArgs(args);
        try {
            tg.setUp();
            tg.generateOutOfLineInserts();
            tg.generateInLineInserts();
        } catch (Exception e) {
            System.err.printf("Unexpected exception: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

    private TableConfig makeTable(Database db, String name, int size) {
        final TableConfig tConfig = new TableConfig(name, db, size);
        return tConfig;
    }

    private TableConfig makeDMLTable(Database db, int nRows) {
        final TableConfig tConfig = new TableConfig("DMLOUTPUT", db, new Object[][] { { nRows } } );
        return tConfig;
    }
    private void generateInLineInserts() {
        return;
    }

    private void generateOutOfLineInserts() throws Exception {
        Database db = getDatabase();
        final int NROWS = 1000000;

        final TableConfig alphaConfig = makeTable(db, "ALPHA", NROWS);
        final TableConfig betaConfig = makeTable(db, "BETA", 0);
        // This is a DML output.  It has one row and one column
        // whose value is NROWS.  It looks like makeTable, but it's
        // actually not like it at all.
        final TableConfig dmlOutput = makeDMLTable(db, NROWS);
        DBConfig PDB = new DBConfig(getClass(),
                                    GenerateEETests.class.getResource(DDL_FILENAME),
                                    getCatalogString(),
                                    alphaConfig,
                                    betaConfig,
                                    dmlOutput);
        PDB.addTest(new TestConfig("perf_test_outline_insert",
                                   "INSERT INTO BETA SELECT * FROM ALPHA;",
                                   false,
                                   dmlOutput).setFragmentNumber(1));
        generateTests("perfplans", "TestInlineInsertPerfPlans", PDB);
    }

}
