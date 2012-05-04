/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.ClientResponseImpl;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.ExpectedProcedureException;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.utils.LogKeys;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;

abstract public class ProcedureTask extends SiteTasker
{
    static final VoltLogger execLog = new VoltLogger("EXEC");
    static final VoltLogger hostLog = new VoltLogger("HOST");

    final ProcedureRunner m_runner;
    final InitiatorMailbox m_initiator;
    final TransactionState m_txn;

    ProcedureTask(InitiatorMailbox initiator, ProcedureRunner runner, TransactionState txn)
    {
        m_initiator = initiator;
        m_runner = runner;
        m_txn = txn;
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    abstract public void run(SiteProcedureConnection siteConnection);

    /** procedure tasks must complete their txnstates */
    abstract void completeInitiateTask(SiteProcedureConnection siteConnection);

    /** Priority returns this task's priority for scheduling */
    @Override
    public int priority()
    {
        return 0;
    }

    /** Mostly copy-paste of old ExecutionSite.processInitiateTask() */
    protected InitiateResponseMessage processInitiateTask(Iv2InitiateTaskMessage task)
    {
        final InitiateResponseMessage response = new InitiateResponseMessage(task);

        try {
            Object[] callerParams = null;
            /*
             * Parameters are lazily deserialized. We may not find out until now
             * that the parameter set is corrupt
             */
            try {
                callerParams = task.getParameters();
            } catch (RuntimeException e) {
                Writer result = new StringWriter();
                PrintWriter pw = new PrintWriter(result);
                e.printStackTrace(pw);
                response.setResults(
                        new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                            new VoltTable[] {},
                            "Exception while deserializing procedure params\n" +
                            result.toString()));
            }
            if (callerParams != null) {
                ClientResponseImpl cr = null;

                // find the txn id visible to the proc
                long txnId = m_txn.txnId;
                StoredProcedureInvocation invocation = m_txn.getInvocation();
                if ((invocation != null) && (invocation.getType() == ProcedureInvocationType.REPLICATED)) {
                    txnId = invocation.getOriginalTxnId();
                }

                m_runner.setupTransaction(m_txn);
                /*
                   if (m_runner.isSystemProcedure()) {
                       final Object[] combinedParams = new Object[callerParams.length + 1];
                       combinedParams[0] = m_systemProcedureContext;
                       for (int i=0; i < callerParams.length; ++i) combinedParams[i+1] = callerParams[i];
                       cr = runner.call(txnId, combinedParams);
                   }
                   else {
                        cr = runner.call(txnId, task.getParameters());
                    }
                */
                cr = m_runner.call(txnId, task.getParameters());

                response.setResults(cr);
                // record the results of write transactions to the transaction state
                // this may be used to verify the DR replica cluster gets the same value
                // skip for multi-partition txns because only 1 of k+1 partitions will
                //  have the real results
                if ((!task.isReadOnly()) && task.isSinglePartition()) {
                    m_txn.storeResults(cr);
                }
            }
        }
        catch (final ExpectedProcedureException e) {
            execLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_ExpectedProcedureException.name(), e);
            response.setResults(
                    new ClientResponseImpl(
                        ClientResponse.GRACEFUL_FAILURE,
                        new VoltTable[]{},
                        e.toString()));
        }
        catch (final Exception e) {
            // Should not be able to reach here. VoltProcedure.call caught all invocation target exceptions
            // and converted them to error responses. Java errors are re-thrown, and not caught by this
            // exception clause. A truly unexpected exception reached this point. Crash. It's a defect.
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_UnexpectedProcedureException.name(), e);
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        return response;
    }


}
