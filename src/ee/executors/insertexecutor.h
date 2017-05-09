/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef HSTOREINSERTEXECUTOR_H
#define HSTOREINSERTEXECUTOR_H

#include "common/Pool.hpp"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/valuevector.h"
#include "executors/abstractexecutor.h"
#include "expressions/functionexpression.h"
#include "plannodes/insertnode.h"
#include "storage/persistenttable.h"
#include "storage/ConstraintFailureException.h"

namespace voltdb {

class InsertPlanNode;
class TempTable;

/**
 * This is the executor for insert nodes.
 */
class InsertExecutor : public AbstractExecutor
{
 public:
 InsertExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
     : AbstractExecutor(engine, abstract_node),
        m_node(NULL),
        m_inputTable(NULL),
        m_partitionColumn(-1),
        m_multiPartition(false),
        m_isStreamed(false),
        m_hasStreamView(false),
        m_isUpsert(false),
        m_sourceIsPartitioned(false),
        m_hasPurgeFragment(false),
        m_templateTupleStorage(),
        m_nowFields(),
        m_targetTable(NULL),
        m_modifiedTuples(0),
        m_count_tuple(),
        m_persistentTable(NULL),
        m_upsertTuple(),
        m_templateTuple(),
        m_tempPool(NULL)
            {
            }

    bool p_execute_init(const TupleSchema *inputSchema,
                        TempTable *newOutputTable);
    void p_execute_finish();
    void p_execute_tuple(TableTuple &tuple);

    Table *getTargetTable() {
        return m_targetTable;
    }
 protected:
    bool p_init(AbstractPlanNode*,
                TempTableLimits* limits);
    bool p_execute(const NValueArray &params);


    InsertPlanNode* m_node;
    TempTable* m_inputTable;

    int m_partitionColumn;
    bool m_multiPartition;
    bool m_isStreamed;
    bool m_hasStreamView;
    bool m_isUpsert;
    bool m_sourceIsPartitioned;
    bool m_hasPurgeFragment;

 private:

    /** If the table is at or over its tuple limit, this method
     * executes the purge fragment for the table.  Returns true if
     * nothing went wrong (regardless of whether the purge
     * fragment was executed) and false otherwise.
     *
     * The purge fragment might perform a truncate table,
     * in which case the persistent table object we're inserting
     * into might change.  Passing a pointer-to-pointer allows
     * the callee to update the persistent table pointer.
     */
    void executePurgeFragmentIfNeeded(PersistentTable** table);

    /** A tuple with the target table's schema that is populated
     * with default values for each field. */
    StandAloneTupleStorage m_templateTupleStorage;

    /** A memory pool for allocating non-inlined varchar and
     * varbinary default values */
    Pool m_memoryPool;

    /** A list of indexes of each column in the template tuple
     * that has a DEFAULT of NOW, which must be set on each
     * execution of this plan. */
    std::vector<int> m_nowFields;
    /*
     * These are logically local variables to p_execute.
     * But they are shared between p_execute and p_execute_init.
     */
    Table* m_targetTable;
    int m_modifiedTuples;
    TableTuple m_count_tuple;
    PersistentTable* m_persistentTable;
    TableTuple m_upsertTuple;
    TableTuple m_templateTuple;
    Pool* m_tempPool;
};

/**
 * Given an abstract plan node, extract an inline InsertExecutor
 * for its InlineInsertPlanNode if there be any.
 */
InsertExecutor *getInlineInsertExecutor(const AbstractPlanNode *node);

inline bool InsertExecutor::p_execute_init(const TupleSchema *inputSchema,
                                    TempTable *newOutputTable) {
    assert(m_node == dynamic_cast<InsertPlanNode*>(m_abstractNode));
    assert(m_node);
    assert(inputSchema);
    assert(m_node->isInline() || (m_inputTable == dynamic_cast<TempTable*>(m_node->getInputTable())));
    assert(m_node->isInline() || m_inputTable);


    // Target table can be StreamedTable or PersistentTable and must not be NULL
    // Update target table reference from table delegate
    m_targetTable = m_node->getTargetTable();
    assert(m_targetTable);
    assert((m_targetTable == dynamic_cast<PersistentTable*>(m_targetTable)) ||
            (m_targetTable == dynamic_cast<StreamedTable*>(m_targetTable)));

    m_persistentTable = m_isStreamed ?
        NULL : static_cast<PersistentTable*>(m_targetTable);
    m_upsertTuple = TableTuple(m_targetTable->schema());

    VOLT_TRACE("INPUT TABLE: %s\n", m_node->isInline() ? "INLINE" : m_inputTable->debug().c_str());

    // count the number of successful inserts
    m_modifiedTuples = 0;

    m_tmpOutputTable = newOutputTable;
    assert(m_tmpOutputTable);
    m_count_tuple = m_tmpOutputTable->tempTuple();

    // For export tables with no partition column,
    // if the data is from a replicated source,
    // only insert into one partition (the one for hash(0)).
    // Other partitions can just return a 0 modified tuple count.
    // OTOH, if the data is coming from a (sub)query with
    // partitioned tables, perform the insert on every partition.
    if (m_partitionColumn == -1 &&
            m_isStreamed &&
            m_multiPartition &&
            !m_sourceIsPartitioned &&
            !m_engine->isLocalSite(ValueFactory::getBigIntValue(0L))) {
        m_count_tuple.setNValue(0, ValueFactory::getBigIntValue(0L));
        // put the tuple into the output table
        m_tmpOutputTable->insertTuple(m_count_tuple);
        return true;
    }
    m_templateTuple = m_templateTupleStorage.tuple();

    std::vector<int>::iterator it;
    for (it = m_nowFields.begin(); it != m_nowFields.end(); ++it) {
        m_templateTuple.setNValue(*it, NValue::callConstant<FUNC_CURRENT_TIMESTAMP>());
    }

    VOLT_DEBUG("This is a %s insert on partition with id %d",
               m_node->isInline() ? "inline"
                       : (m_node->getChildren()[0]->getPlanNodeType() == PLAN_NODE_TYPE_MATERIALIZE ?
                               "single-row" : "multi-row"),
               m_engine->getPartitionId());
    VOLT_DEBUG("Offset of partition column is %d", m_partitionColumn);
    m_tempPool = ExecutorContext::getTempStringPool();
    return false;
}

inline void InsertExecutor::executePurgeFragmentIfNeeded(PersistentTable** ptrToTable) {
    PersistentTable* table = *ptrToTable;
    int tupleLimit = table->tupleLimit();
    int numTuples = table->visibleTupleCount();

    // Note that the number of tuples may be larger than the limit.
    // This can happen we data is redistributed after an elastic
    // rejoin for example.
    if (numTuples >= tupleLimit) {
        // Next insert will fail: run the purge fragment
        // before trying to insert.
        m_engine->executePurgeFragment(table);

        // If the purge fragment did a truncate table, then the old
        // table is still around for undo purposes, but there is now a
        // new empty table we can insert into.  Update the caller's table
        // pointer to use it.
        //
        // The plan node will go through the table catalog delegate to get
        // the correct instance of PersistentTable.
        *ptrToTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    }
}

inline void InsertExecutor::p_execute_tuple(TableTuple &tuple) {
    const std::vector<int>& fieldMap = m_node->getFieldMap();
    std::size_t mapSize = fieldMap.size();

    for (int i = 0; i < mapSize; ++i) {
        // Most executors will just call setNValue instead of
        // setNValueAllocateForObjectCopies.
        //
        // However, We need to call
        // setNValueAllocateForObjectCopies here.  Sometimes the
        // input table's schema has an inlined string field, and
        // it's being assigned to the target table's outlined
        // string field.  In this case we need to tell the NValue
        // where to allocate the string data.
        // For an "upsert", this templateTuple setup has two effects --
        // It sets the primary key column(s) and it sets the
        // updated columns to their new values.
        // If the primary key value (combination) is new, the
        // templateTuple is the exact combination of new values
        // and default values required by the insert.
        // If the primary key value (combination) already exists,
        // only the NEW values stored on the templateTuple get updated
        // in the existing tuple and its other columns keep their existing
        // values -- the DEFAULT values that are stored in templateTuple
        // DO NOT get copied to an existing tuple.
        m_templateTuple.setNValueAllocateForObjectCopies(fieldMap[i],
                                                         tuple.getNValue(i),
                                                         m_tempPool);
    }

    VOLT_TRACE("Inserting tuple '%s' into target table '%s' with table schema: %s",
               m_templateTuple.debug(m_targetTable->name()).c_str(), m_targetTable->name().c_str(),
               m_targetTable->schema()->debug().c_str());

    // If there is a partition column for the target table
    if (m_partitionColumn != -1) {
        // get the value for the partition column
        NValue value = m_templateTuple.getNValue(m_partitionColumn);
        bool isLocal = m_engine->isLocalSite(value);

        // if it doesn't map to this partiton
        if (!isLocal) {
            if (m_multiPartition) {
                // The same row is presumed to also be generated
                // on some other partition, where the partition key
                // belongs.
                return;
            }
            // When a streamed table has no views, let an SP insert execute.
            // This is backward compatible with when there were only export
            // tables with no views on them.
            // When there are views, be strict and throw mispartitioned
            // tuples to force partitioned data to be generated only
            // where partitoned view rows are maintained.
            if (!m_isStreamed || m_hasStreamView) {
                throw ConstraintFailureException(
                                                 m_targetTable, m_templateTuple,
                                                 "Mispartitioned tuple in single-partition insert statement.");
            }
        }
    }

    if (m_isUpsert) {
        // upsert execution logic
        assert(m_persistentTable->primaryKeyIndex() != NULL);
        TableTuple existsTuple = m_persistentTable->lookupTupleByValues(m_templateTuple);

        if (!existsTuple.isNullTuple()) {
            // The tuple exists already, update (only) the templateTuple columns
            // that were initialized from the input tuple via the field map.
            // Technically, this includes setting primary key values,
            // but they are getting set to equivalent values, so that's OK.
            // A simple setNValue works here because any required object
            // allocations were handled when copying the input values into
            // the templateTuple.
            m_upsertTuple.move(m_templateTuple.address());
            TableTuple &tempTuple = m_persistentTable->copyIntoTempTuple(existsTuple);
            for (int i = 0; i < mapSize; ++i) {
                tempTuple.setNValue(fieldMap[i],
                                    m_templateTuple.getNValue(fieldMap[i]));
            }

            m_persistentTable->updateTupleWithSpecificIndexes(existsTuple, tempTuple,
                                                              m_persistentTable->allIndexes());
            // successfully updated
            ++m_modifiedTuples;
            return;
        }
        // else, the primary key did not match,
        // so fall through to the "insert" logic
    }

    // try to put the tuple into the target table
    if (m_hasPurgeFragment) {
        executePurgeFragmentIfNeeded(&m_persistentTable);
        // purge fragment might have truncated the table, and
        // refreshed the persistent table pointer.  Make sure to
        // use it when doing the insert below.
        m_targetTable = m_persistentTable;
    }
    m_targetTable->insertTuple(m_templateTuple);
    VOLT_DEBUG("Target table:\n%s\n", m_targetTable->debug().c_str());
    // successfully inserted
    ++m_modifiedTuples;
    return;
}

inline void InsertExecutor::p_execute_finish() {
    m_count_tuple.setNValue(0, ValueFactory::getBigIntValue(m_modifiedTuples));
    // put the tuple into the output table
    m_tmpOutputTable->insertTuple(m_count_tuple);

    // add to the planfragments count of modified tuples
    m_engine->addToTuplesModified(m_modifiedTuples);
    VOLT_DEBUG("Finished inserting %d tuples", m_modifiedTuples);
    VOLT_DEBUG("InsertExecutor output table:\n%s\n", m_tmpOutputTable->debug().c_str());
    VOLT_DEBUG("InsertExecutor target table:\n%s\n", m_targetTable->debug().c_str());
}

}

namespace voltdb {
inline InsertExecutor *getInlineInsertExecutor(const AbstractPlanNode *node) {
    InsertExecutor *answer = NULL;
    InsertPlanNode *insertNode = dynamic_cast<InsertPlanNode *>(node->getInlinePlanNode(PLAN_NODE_TYPE_INSERT));
    if (insertNode) {
        answer = dynamic_cast<InsertExecutor *>(insertNode->getExecutor());
    }
    return answer;
}
}

#endif
