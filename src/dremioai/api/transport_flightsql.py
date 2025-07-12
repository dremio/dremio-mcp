"""
Flight SQL transport implementation for Dremio Cloud.

This module provides a transport layer that uses Apache Arrow Flight SQL
to connect to Dremio Cloud, which requires Flight SQL instead of basic Flight.
"""

import asyncio
from typing import Optional
import pandas as pd
import adbc_driver_flightsql.dbapi as flight_sql
from dremioai.log import logger


class FlightSQLTransport:
    """Transport implementation using Apache Arrow Flight SQL for Dremio Cloud."""
    
    def __init__(self, uri: str, pat: str, project_id: Optional[str] = None):
        """
        Initialize Flight SQL transport.
        
        Args:
            uri: Dremio URI (e.g., https://api.eu.dremio.cloud)
            pat: Personal Access Token for authentication
            project_id: Optional project ID for Dremio Cloud
        """
        self.uri = uri
        self.pat = pat
        self.project_id = project_id
        
        # Convert HTTP URI to Flight SQL URI
        if uri.startswith("https://"):
            self.flight_uri = uri.replace("https://", "grpc+tls://") + ":443"
        elif uri.startswith("http://"):
            self.flight_uri = uri.replace("http://", "grpc://")
        else:
            self.flight_uri = uri
            
        self._connection = None
        
        logger().info(f"Initialized Flight SQL transport for {self.flight_uri}")

    async def _get_connection(self):
        """Get or create Flight SQL connection."""
        if self._connection is None:
            try:
                # Prepare database kwargs for ADBC Flight SQL driver
                db_kwargs = {
                    "adbc.flight.sql.authorization_header": f"Bearer {self.pat}",
                }
                
                # TODO: Add project ID support - need to find correct parameter name
                # The 'adbc.flight.sql.catalog' parameter is not supported by this driver version
                if self.project_id:
                    logger().warning(f"Project ID specified ({self.project_id}) but catalog parameter not yet supported in Flight SQL driver")
                
                logger().info(f"Connecting to Flight SQL endpoint: {self.flight_uri}")
                
                # Create connection using ADBC Flight SQL driver with correct API
                self._connection = flight_sql.connect(uri=self.flight_uri, db_kwargs=db_kwargs)
                
                logger().info("Flight SQL connection established successfully")
                
            except Exception as e:
                logger().error(f"Failed to establish Flight SQL connection: {e}")
                raise RuntimeError(f"Flight SQL connection failed: {e}")
        
        return self._connection

    async def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query using Flight SQL and return results as DataFrame"""
        try:
            connection = await self._get_connection()
            
            logger().info(f"Executing Flight SQL query: {sql}")
            
            # Execute query
            cursor = connection.cursor()
            cursor.execute(sql)
            
            # Try different methods to fetch results, handling schema inconsistencies
            try:
                # First try fetch_df() which might handle schema inconsistencies better
                df = cursor.fetch_df()
            except Exception as df_error:
                logger().warning(f"fetch_df() failed: {df_error}")
                try:
                    # Try fetchall() which returns rows as tuples
                    logger().info("Trying fetchall() method...")
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    
                    # Convert to pandas DataFrame manually
                    import pandas as pd
                    df = pd.DataFrame(rows, columns=columns)
                    logger().info(f"Successfully fetched {len(df)} rows using fetchall()")
                    
                except Exception as fetchall_error:
                    logger().warning(f"fetchall() failed: {fetchall_error}")
                    # Last resort: try fetch_arrow_table()
                    logger().info("Trying fetch_arrow_table() as last resort...")
                    arrow_table = cursor.fetch_arrow_table()
                    df = arrow_table.to_pandas()
            
            logger().info(f"Flight SQL query executed successfully, returned {len(df)} rows")
            return df
            
        except Exception as e:
            logger().error(f"Flight SQL query execution failed: {e}")
            raise RuntimeError(f"Flight SQL query execution failed: {e}")

    async def close(self):
        """Close the Flight SQL connection."""
        if self._connection:
            try:
                self._connection.close()
                logger().info("Flight SQL connection closed")
            except Exception as e:
                logger().warning(f"Error closing Flight SQL connection: {e}")
            finally:
                self._connection = None
