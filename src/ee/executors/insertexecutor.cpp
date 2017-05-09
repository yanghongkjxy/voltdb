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

#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/types.h"
#include "execution/VoltDBEngine.h"
#include "expressions/functionexpression.h"
#include "insertexecutor.h"
#include "plannodes/insertnode.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"

#include <vector>
#include <set>

using namespace std;
using namespace voltdb;

bool InsertExecutor::p_init(AbstractPlanNode* abstractNode,
                            TempTableLimits* limits)
{
    VOLT_TRACE("init Insert Executor");

    m_node = dynamic_cast<InsertPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());
    assert(m_node->getInputTableCount() == (m_node->isInline() ? 0 : 1));

    Table* targetTable = m_node->getTargetTable();
    m_isUpsert = m_node->isUpsert();

    //
    // The insert node's input schema is fixed.  But
    // if this is an inline node we don't set it here.
    // We let the parent node set it in p_execute_init.
    //
    // Also, we don't want to set the input table for inline
    // insert nodes.
    //
    if ( ! m_node->isInline()) {
        setDMLCountOutputTable(limits);
        m_inputTable = dynamic_cast<TempTable*>(m_node->getInputTable()); //input table should be temptable
        assert(m_inputTable);
    } else {
        m_inputTable = NULL;
    }

    // Target table can be StreamedTable or PersistentTable and must not be NULL
    PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(targetTable);
    m_partitionColumn = -1;
    StreamedTable *streamTarget = dynamic_cast<StreamedTable*>(targetTable);
    m_hasStreamView = false;
    if (streamTarget != NULL) {
        m_isStreamed = true;
        //See if we have any views.
        m_hasStreamView = streamTarget->hasViews();
        m_partitionColumn = streamTarget->partitionColumn();
    }
    if (m_isUpsert) {
        VOLT_TRACE("init Upsert Executor actually");
        assert( ! m_node->isInline() );
        if (m_isStreamed) {
            VOLT_ERROR("UPSERT is not supported for Stream table %s", targetTable->name().c_str());
        }
        // look up the tuple whether it exists already
        if (persistentTarget->primaryKeyIndex() == NULL) {
            VOLT_ERROR("No primary keys were found in our target table '%s'",
                    targetTable->name().c_str());
        }
    }

    if (persistentTarget) {
        m_partitionColumn = persistentTarget->partitionColumn();
    }

    m_multiPartition = m_node->isMultiPartition();

    m_sourceIsPartitioned = m_node->sourceIsPartitioned();

    // allocate memory for template tuple, set defaults for all columns
    m_templateTupleStorage.init(targetTable->schema());


    TableTuple tuple = m_templateTupleStorage.tuple();

    std::set<int> fieldsExplicitlySet(m_node->getFieldMap().begin(), m_node->getFieldMap().end());
    // These default values are used for an INSERT including the INSERT sub-case of an UPSERT.
    // The defaults are purposely ignored in favor of existing column values
    // for the UPDATE subcase of an UPSERT.
    m_node->initTupleWithDefaultValues(m_engine,
                                       &m_memoryPool,
                                       fieldsExplicitlySet,
                                       tuple,
                                       m_nowFields);
    m_hasPurgeFragment = persistentTarget ? persistentTarget->hasPurgeFragment() : false;

    return true;
}

bool InsertExecutor::p_execute(const NValueArray &params) {
    //
    // See p_execute_init above.  If we are inserting a
    // replicated table, we only insert on one site.  So
    // we will be done on all the other sites.
    //
    const TupleSchema *inputSchema = m_inputTable->schema();
    if (p_execute_init(inputSchema, m_tmpOutputTable)) {
        return true;
    }

    //
    // An insert is quite simple really. We just loop through our m_inputTable
    // and insert any tuple that we find into our targetTable. It doesn't get any easier than that!
    //
    TableIterator iterator = m_inputTable->iterator();
    TableTuple inputTuple = TableTuple(inputSchema);
    while (iterator.next(inputTuple)) {
        p_execute_tuple(inputTuple);
    }

    p_execute_finish();
    return true;
}

