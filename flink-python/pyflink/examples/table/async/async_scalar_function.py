################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Example demonstrating the usage of Python Async Scalar Functions in PyFlink.

This example shows how to use AsyncScalarFunction for asynchronous operations 
such as database lookups, REST API calls, or other I/O-bound operations that 
would benefit from async execution.
"""

import asyncio
import logging
import random
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import AsyncScalarFunction, udf


# Example 1: Class-based Async Scalar Function
class AsyncDatabaseLookup(AsyncScalarFunction):
    """
    Simulates an async database lookup operation.
    In real scenarios, this would interact with an async database client.
    """
    
    def open(self, function_context):
        # Initialize resources (e.g., database connection pool)
        self.cache = {}

    async def eval(self, key: str) -> str:
        # Check cache first
        if key in self.cache:
            return self.cache[key]
        
        # Simulate async database query
        await asyncio.sleep(0.1)  # Simulate I/O delay
        
        # Generate result
        value = f"db_value_for_{key}"
        self.cache[key] = value
        return value
    
    def close(self):
        # Clean up resources
        self.cache.clear()


# Example 2: Decorator-based Async Scalar Function
@udf(result_type=DataTypes.STRING())
async def async_api_call(product_id: str) -> str:
    """
    Simulates an async REST API call to fetch product information.
    """
    # Simulate API call delay
    await asyncio.sleep(0.05)
    
    # Simulate API response
    price = random.randint(10, 1000)
    return f"Product {product_id}: ${price}"


# Example 3: Async function with multiple parameters
@udf(
    input_types=[DataTypes.STRING(), DataTypes.INT()],
    result_type=DataTypes.STRING()
)
async def async_enrichment(user_id: str, score: int) -> str:
    """
    Enriches user data by combining multiple async lookups.
    """
    # Simulate async operations
    await asyncio.sleep(0.03)
    
    # Return enriched data
    category = "premium" if score > 80 else "standard"
    return f"User {user_id} ({category})"


def async_scalar_function_example():
    """
    Main example showing how to register and use async scalar functions.
    """
    # Create table environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    
    # Register the async scalar functions
    t_env.create_temporary_function("async_db_lookup", AsyncDatabaseLookup())
    t_env.create_temporary_function("async_api_call", async_api_call)
    t_env.create_temporary_function("async_enrichment", async_enrichment)
    
    # Create a source table
    t_env.execute_sql("""
        CREATE TABLE source_table (
            user_id STRING,
            product_id STRING,
            score INT
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '10',
            'fields.user_id.length' = '1',
            'fields.product_id.length' = '1',
            'fields.score.min' = '50',
            'fields.score.max' = '100'
        )
    """)
    
    print("Created source table with datagen connector\n")
    
    # Use async scalar functions in SQL queries
    print("Example Query 1: Using async_db_lookup")
    result1 = t_env.sql_query("""
        SELECT 
            user_id,
            async_db_lookup(user_id) as user_info
        FROM source_table
        LIMIT 5
    """)
    print(result1.to_pandas())
    print()
    
    print("Example Query 2: Using async_api_call")
    result2 = t_env.sql_query("""
        SELECT 
            product_id,
            async_api_call(product_id) as product_info
        FROM source_table
        LIMIT 5
    """)
    print(result2.to_pandas())
    print()
    
    print("Example Query 3: Using async_enrichment")
    result3 = t_env.sql_query("""
        SELECT 
            user_id,
            score,
            async_enrichment(user_id, score) as enriched_data
        FROM source_table
        LIMIT 5
    """)
    print(result3.to_pandas())
    print()
    
    print("=== Async Scalar Function Example Completed ===")


# Example 4: Using udf in Table API
def table_api_example():
    """
    Example showing how to use async scalar functions in Table API.
    """
    from pyflink.table import expressions as expr
    
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)
    
    # Create async UDF
    async_lookup = udf(
        AsyncDatabaseLookup(),
        result_type=DataTypes.STRING(),
        name="async_lookup"
    )
    
    # Create source table
    t_env.execute_sql("""
        CREATE TABLE users (
            id STRING,
            name STRING
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '5'
        )
    """)
    
    # Use async UDF in Table API
    table = t_env.from_path('users')
    result = table.select(
        expr.col('id'),
        async_lookup(expr.col('id')).alias('user_details')
    )
    
    print("\n=== Table API Example ===")
    print(result.to_pandas())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    async_scalar_function_example()
    table_api_example()
