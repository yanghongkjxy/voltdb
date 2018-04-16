/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;


public class ViolateUniquenessAndCatchException extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO NEW_ORDER VALUES (?, ?, ?);");

    public long run(long no_o_id, long no_d_id, byte no_w_id) {
        voltQueueSQL(insert, no_o_id, no_d_id, no_w_id);
        long count = voltExecuteSQL()[0].asScalarLong();
        long retval = count;
        try {
            voltQueueSQL(insert, no_o_id, no_d_id, no_w_id);
            count = voltExecuteSQL()[0].asScalarLong();
            retval += count;
        } catch (org.voltdb.exceptions.ConstraintFailureException e) {
            String description = e.toString();
            System.out.println(description);
        }
        /*catch (Exception e) {
            e.printStackTrace();
        }*/

        return retval;
    }
}
