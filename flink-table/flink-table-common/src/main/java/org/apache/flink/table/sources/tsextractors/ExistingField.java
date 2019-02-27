/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources.tsextractors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;

import java.util.Arrays;

import static org.apache.flink.table.expressions.FunctionDefinitions.CAST;

/**
 * Converts an existing {@link Long}, {@link java.sql.Timestamp}, or
 * timestamp formatted {@link String} field (e.g., "2018-05-28 12:34:56.000") into
 * a rowtime attribute.
 */
@PublicEvolving
public final class ExistingField extends TimestampExtractor {

	/**
	 * The field to convert into a rowtime attribute.
	 */
	private final String field;

	public ExistingField(String field) {
		this.field = field;
	}

	@Override
	public String[] getArgumentFields() {
		return new String[]{field};
	}

	@Override
	public void validateArgumentFields(TypeInformation<?>[] argumentFieldTypes) {
		TypeInformation<?> fieldType = argumentFieldTypes[0];
		if (!fieldType.equals(Types.LONG) && !fieldType.equals(Types.SQL_TIMESTAMP) &&
			!fieldType.equals(Types.STRING)) {
			throw new ValidationException("Field '" + field + "' must be of type Long or " +
				"Timestamp or String but is of type + " + fieldType + ".");
		}
	}

	/**
	 * Returns an {@link Expression} that casts a {@link Long}, {@link java.sql.Timestamp}, or
	 * timestamp formatted {@link String} field (e.g., "2018-05-28 12:34:56.000")
	 * into a rowtime attribute.
	 */
	@Override
	public Expression getExpression(
		FieldReferenceExpression[] fieldAccesses,
		TypeInformation<?>[] fieldTypes) {
		FieldReferenceExpression fieldAccess = fieldAccesses[0];
		TypeInformation<?> fieldType = fieldTypes[0];

		FieldReferenceExpression fieldAccessWithType =
			new FieldReferenceExpression(fieldAccess.getName(), Types.LONG);
		if (fieldType.equals(Types.LONG)) {
			// access LONG field
			return fieldAccessWithType;
		} else if (fieldType.equals(Types.SQL_TIMESTAMP)) {
			// cast timestamp to long
			return new CallExpression(
				CAST,
				Arrays.asList(fieldAccessWithType, new TypeLiteralExpression(Types.LONG)));
		} else {
			// access STRING field
			return new CallExpression(
				CAST,
				Arrays.asList(
					new CallExpression(CAST,
						Arrays.asList(
							fieldAccessWithType,
							new TypeLiteralExpression(SqlTimeTypeInfo.TIMESTAMP))),
					new TypeLiteralExpression(Types.LONG)));
		}
	}

	@Override
	public boolean equals(Object other) {
		return other instanceof ExistingField && field.equals(((ExistingField) other).field);
	}

	@Override
	public int hashCode() {
		return field.hashCode();
	}
}
