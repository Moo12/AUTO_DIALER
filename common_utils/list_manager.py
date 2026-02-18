"""
List Manager Module

Manages the 3-step process for importing Excel data and creating lists:
1. Upload Excel to MySQL table
2. Create a new list in the lists table
3. Link all inserted users to the new list in special_users_to_lists table
"""

import os
import sys
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from common_utils.db_connection import DatabaseConnection
from common_utils.excel_handler import ExcelHandler
from common_utils.config_manager import ConfigManager


class ListManager:
    """
    Manages the 3-step process for importing Excel data and creating lists.
    """
    
    def __init__(self, list_type: str, db_connection: DatabaseConnection, config_manager: ConfigManager):
        """
        Initialize List Manager.
        
        Args:
            list_type: Type key that matches data_base_tables configuration
            db_connection: DatabaseConnection instance for MySQL operations
            config_path: Optional path to config.yaml (defaults to settings_backend/config.yaml)
        """
        self.db = db_connection
        self.excel_handler = ExcelHandler(db_connection)
        self.list_type = list_type  # Store list_type as instance variable

        self.config_manager = config_manager
        self.config = self.config_manager.get_config()
        
        # Load table configurations from data_base_tables
        self.data_base_tables = self.config.get('data_base_tables', {}).get(list_type, {})

        # Load table names from config
        users_config = self.data_base_tables.get('users', {})
        lists_config = self.data_base_tables.get('lists', {})
        users_to_lists_config = self.data_base_tables.get('users_to_lists', {})
        
        self.users_table = users_config.get('table_name', "special_users")
        self.lists_table = lists_config.get('table_name', "list_special_users")
        self.users_to_lists_table = users_to_lists_config.get('table_name', "user_list_mapping")
        
        # Load field names from config
        self.user_primary_key = users_config.get('primary_key', 'id')
        self.list_primary_key = lists_config.get('primary_key', 'id')
        self.list_id_field = lists_config.get('id_field', 'id')
        self.list_name_field = lists_config.get('list_name_field', 'list_name')  # Default if not in config
        self.list_active_field = lists_config.get('active_field', 'is_active')
        self.foreign_key_user_id = users_to_lists_config.get('foreign_key_user_id', 'user_id')
        self.foreign_key_list_id = users_to_lists_config.get('foreign_key_list_id', 'list_id')
        
    
    def import_excel_and_create_list(
        self,
        file_path: str,
        list_name: str,
        sheet_name: Optional[str] = None,
        mapping: Optional[Dict[str, str]] = None,
        header_row: int = 0,
        start_row: int = 1,
        update_on_duplicate: bool = True,
        batch_size: int = 100,
        column_converters: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Execute the 3-step process:
        1. Upload Excel to MySQL table
        2. Create a new list in the lists table
        3. Link all inserted users to the new list
        
        Args:
            file_path: Path to Excel file
            table_name: Name of MySQL table to import data into (e.g., 'special_users')
            list_type: Type key that matches data_base_tables configuration
            list_name: Name for the new list to create
            sheet_name: Optional name of sheet to read (None = first sheet)
            mapping: Optional dictionary mapping Excel column names to MySQL column names
            header_row: Row number containing headers (0-indexed, default: 0)
            start_row: Row number where data starts (0-indexed, default: 1)
            update_on_duplicate: If True, update existing rows based on primary key (default: True)
            batch_size: Number of rows to process in each batch (default: 100)
            
        Returns:
            Dictionary with operation results:
                {
                    'success': bool,
                    'step1_result': {...},  # Excel import result
                    'step2_result': {...},  # List creation result
                    'step3_result': {...},  # User-list linking result
                    'list_id': int,         # ID of the created list
                    'rows_inserted': List[Any],  # Primary keys of inserted users
                    'rows_updated': List[Any],   # Primary keys of updated users
                    'rows_linked': int,     # Number of users linked to the list
                    'errors': List[Dict[str, Any]],
                    'total_rows': int
                }
        """
        errors = []
        all_inserted_pks = []
        all_updated_pks = []
        
        # Step 1: Upload Excel to MySQL
        try:
            step1_result = self.excel_handler.excel_to_mysql(
                file_path=file_path,
                table_name=self.users_table,
                sheet_name=sheet_name,
                mapping=mapping,
                header_row=header_row,
                start_row=start_row,
                update_on_duplicate=update_on_duplicate,
                batch_size=batch_size,
                column_converters=column_converters
            )
            
            if not step1_result.get('success', False):
                return {
                    'success': False,
                    'step1_result': step1_result,
                    'step2_result': None,
                    'step3_result': None,
                    'list_id': None,
                    'rows_inserted': [],
                    'rows_updated': [],
                    'rows_linked': 0,
                    'errors': step1_result.get('errors', []),
                    'total_rows': step1_result.get('total_rows', 0)
                }
            
            all_inserted_pks = step1_result.get('rows_inserted', [])
            all_updated_pks = step1_result.get('rows_updated', [])
            errors.extend(step1_result.get('errors', []))
            
        except Exception as e:
            error_msg = f"Step 1 (Excel import) failed: {str(e)}"
            errors.append({'step': 1, 'error': error_msg})
            return {
                'success': False,
                'step1_result': {'success': False, 'error': error_msg},
                'step2_result': None,
                'step3_result': None,
                'list_id': None,
                'rows_inserted': [],
                'rows_updated': [],
                'rows_linked': 0,
                'errors': errors,
                'total_rows': 0
            }
        
        # Step 2: Create a new list in the lists table
        try:
            # Prepare list data dictionary
            list_data = {
                self.list_name_field: list_name,
                self.list_active_field: 0  # Default value
            }
            list_id = self._create_list(list_data)
            step2_result = {
                'success': True,
                'list_id': list_id,
                'list_name': list_name
            }
        except Exception as e:
            error_msg = f"Step 2 (List creation) failed: {str(e)}"
            errors.append({'step': 2, 'error': error_msg})
            return {
                'success': False,
                'step1_result': step1_result,
                'step2_result': {'success': False, 'error': error_msg},
                'step3_result': None,
                'list_id': None,
                'rows_inserted': all_inserted_pks,
                'rows_updated': all_updated_pks,
                'rows_linked': 0,
                'errors': errors,
                'total_rows': step1_result.get('total_rows', 0)
            }
        
        # Step 3: Link all inserted users to the new list
        try:
            # Get all user IDs that were inserted (not updated)
            # We only link newly inserted users, not updated ones
            rows_linked = self._link_users_to_list(
                user_ids=all_inserted_pks,
                list_id=list_id
            )
            step3_result = {
                'success': True,
                'rows_linked': rows_linked,
                'list_id': list_id
            }

            print(f"step3_result: {step3_result}")
        except Exception as e:
            error_msg = f"Step 3 (User-list linking) failed: {str(e)}"
            errors.append({'step': 3, 'error': error_msg})
            step3_result = {
                'success': False,
                'error': error_msg,
                'rows_linked': 0
            }
        
        # Determine overall success
        success = (
            step1_result.get('success', False) and
            step2_result.get('success', False) and
            step3_result.get('success', False) and
            len(errors) == 0
        )
        
        return {
            'success': success,
            'step1_result': step1_result,
            'step2_result': step2_result,
            'step3_result': step3_result,
            'list_id': list_id,
            'rows_inserted': all_inserted_pks,
            'rows_updated': all_updated_pks,
            'rows_linked': step3_result.get('rows_linked', 0),
            'errors': errors,
            'total_rows': step1_result.get('total_rows', 0)
        }
    
    def _create_list(self, list_data: Dict[str, Any]) -> int:
        """
        Create a new list in the list_special_users table.
        
        Table structure (hardcoded):
        - id: int, auto_increment, primary key
        - list_name: varchar(100), NOT NULL
        - is_active: tinyint(1), default 0
        - created_at: timestamp, default CURRENT_TIMESTAMP
        - time_activate_modify: timestamp, default CURRENT_TIMESTAMP on update
        
        Args:
            list_data: Dictionary with field names as keys and values to insert
                      Required: 'list_name'
                      Optional: 'is_active' (defaults to 0 if not provided)
                      Note: 'id', 'created_at', 'time_activate_modify' are auto-generated
            
        Returns:
            The ID of the created list
            
        Raises:
            Exception: If list creation fails
        """
        # Validate required field
        if self.list_name_field not in list_data:
            raise Exception(f"list_data must contain '{self.list_name_field}' field")
        
        # Use table name from config (defaults to 'list_special_users')
        table_name = self.lists_table
        
        # Build INSERT query using field names from config
        # Only insert fields that are provided in list_data (excluding auto-generated fields)
        insert_fields = []
        insert_params = {}
        
        # list_name is required
        insert_fields.append(self.list_name_field)
        insert_params[self.list_name_field] = list_data[self.list_name_field]
        
        # is_active is optional (defaults to 0)
        if self.list_active_field in list_data:
            insert_fields.append(self.list_active_field)
            insert_params[self.list_active_field] = list_data[self.list_active_field]
        else:
            # Set default value
            insert_fields.append(self.list_active_field)
            insert_params[self.list_active_field] = 0
        
        # Note: id, created_at, and time_activate_modify are auto-generated, so we don't include them
        
        # Build the INSERT query
        columns = ', '.join(insert_fields)
        placeholders = ', '.join([f':{field}' for field in insert_fields])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        try:
            if not self.db._is_connected:
                self.db.connect()
            
            with self.db.get_connection() as conn:
                from sqlalchemy import text
                result = conn.execute(text(insert_query), insert_params)
                conn.commit()
                
                # Get the last insert ID (primary key is auto_increment)
                list_id = result.lastrowid
                if list_id is None or list_id == 0:
                    # If lastrowid is not available, query for the inserted row
                    query = f"SELECT {self.list_primary_key} FROM {table_name} WHERE {self.list_name_field} = :list_name ORDER BY {self.list_primary_key} DESC LIMIT 1"
                    result = conn.execute(text(query), {self.list_name_field: list_data[self.list_name_field]})
                    row = result.fetchone()
                    if row:
                        list_id = row[0]
                    else:
                        raise Exception("Could not retrieve the created list ID")
                
                return list_id
        except Exception as e:
            raise Exception(f"Failed to create list: {str(e)}")
    
    def _link_users_to_list(
        self,
        user_ids: List[Any],
        list_id: int
    ) -> int:
        """
        Link users to a list in the special_users_to_lists table.
        
        Table structure (hardcoded):
        - list_id: int, NOT NULL, PRIMARY KEY (composite)
        - user_id: int, NOT NULL, PRIMARY KEY (composite)
        - added_at: timestamp, default CURRENT_TIMESTAMP (auto-generated)
        
        Args:
            table_name: Name of the users table (e.g., 'special_users') - not used, kept for compatibility
            user_ids: List of user IDs (primary keys) to link
            list_id: ID of the list to link users to
            
        Returns:
            Number of users successfully linked
            
        Raises:
            Exception: If linking fails
        """
        if not user_ids:
            return 0
        
        # Table name from config
        junction_table = self.users_to_lists_table
        
        # Use field names from config
        # Build INSERT query with ON DUPLICATE KEY UPDATE to avoid errors if already linked
        insert_query = f"""
            INSERT INTO {junction_table} ({self.foreign_key_list_id}, {self.foreign_key_user_id})
            VALUES (:{self.foreign_key_list_id}, :{self.foreign_key_user_id})
            ON DUPLICATE KEY UPDATE {self.foreign_key_list_id} = {self.foreign_key_list_id}
        """
        
        rows_linked = 0
        
        try:
            if not self.db._is_connected:
                self.db.connect()
            
            with self.db.get_connection() as conn:
                from sqlalchemy import text
                
                # Iterate through user_ids and insert each one
                for user_id in user_ids:
                    try:
                        params = {self.foreign_key_list_id: list_id, self.foreign_key_user_id: user_id}
                        result = conn.execute(text(insert_query), params)
                        # Check if row was inserted (not updated)
                        # ON DUPLICATE KEY UPDATE returns 2 for update, 1 for insert
                        if result.rowcount > 0:
                            rows_linked += 1
                    except Exception as e:
                        # Log error but continue with other users
                        print(f"Warning: Could not link user_id {user_id} to list_id {list_id}: {e}", file=sys.stderr)
                
                conn.commit()
                
        except Exception as e:
            raise Exception(f"Failed to link users to list: {str(e)}")
        
        return rows_linked
    
    def _get_current_users_in_list(self, conn, list_id: int) -> Dict[int, Dict[str, Any]]:
        """
        Get current users in a list.
        
        Args:
            conn: Database connection
            list_id: ID of the list
            
        Returns:
            Dictionary mapping user_id to user data
        """
        junction_table = self.users_to_lists_table
        users_table = self.users_table
        
        query = f"""
            SELECT u.*
            FROM {users_table} u
            INNER JOIN {junction_table} j ON u.{self.user_primary_key} = j.{self.foreign_key_user_id}
            WHERE j.{self.foreign_key_list_id} = :list_id
        """
        
        from sqlalchemy import text
        result = conn.execute(text(query), {self.foreign_key_list_id: list_id})
        rows = result.fetchall()
        
        # Convert rows to dictionary keyed by user_id
        current_users = {}
        if rows:
            # Get column names
            columns = result.keys()
            for row in rows:
                user_dict = dict(zip(columns, row))
                user_id = user_dict.get(self.user_primary_key)
                if user_id:
                    current_users[user_id] = user_dict
        
        return current_users
    
    def _update_user(self, conn, user_id: int, user_data: Dict[str, Any]) -> bool:
        """
        Update an existing user.
        
        Args:
            conn: Database connection
            user_id: ID of the user to update
            user_data: Dictionary with fields to update (excluding 'id')
            
        Returns:
            True if update succeeded, False otherwise
        """
        if not user_data:
            return False
        
        users_table = self.users_table
        
        # Remove primary key from user_data if present (don't update primary key)
        update_data = {k: v for k, v in user_data.items() if k != self.user_primary_key and k != 'user_id' and k != 'id'}
        
        if not update_data:
            return False
        
        update_fields = []
        update_params = {self.user_primary_key: user_id}
        
        for field, value in update_data.items():
            update_fields.append(f'{field} = :{field}')
            update_params[field] = value
        
        update_query = f"""
            UPDATE {users_table}
            SET {', '.join(update_fields)}
            WHERE {self.user_primary_key} = :{self.user_primary_key}
        """
        
        try:
            from sqlalchemy import text
            result = conn.execute(text(update_query), update_params)
            return result.rowcount > 0
        except Exception as e:
            print(f"Warning: Could not update user_id {user_id}: {e}", file=sys.stderr)
            return False
    
    def _create_user(self, conn, user_data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new user.
        
        Args:
            conn: Database connection
            user_data: Dictionary with user fields (excluding 'id')
            
        Returns:
            New user ID if creation succeeded, None otherwise
        """
        users_table = self.users_table
        
        # Remove primary key from user_data if present
        insert_data = {k: v for k, v in user_data.items() if k != self.user_primary_key and k != 'user_id' and k != 'id'}
        
        if not insert_data:
            return None
        
        columns = ', '.join(insert_data.keys())
        placeholders = ', '.join([f':{key}' for key in insert_data.keys()])
        
        insert_query = f"""
            INSERT INTO {users_table} ({columns})
            VALUES ({placeholders})
        """
        
        try:
            from sqlalchemy import text
            result = conn.execute(text(insert_query), insert_data)
            conn.commit()
            return result.lastrowid
        except Exception as e:
            print(f"Warning: Could not create user: {e}", file=sys.stderr)
            return None
    
    def edit_list(
        self,
        list_id: int,
        list_name: Optional[str] = None,
        is_active: Optional[int] = None,
        users: Optional[List[Dict[str, Any]]] = None,
        add_users_only: bool = False
    ) -> Dict[str, Any]:
        """
        Edit a list: update name, activate/deactivate, sync users.
        
        Hybrid mode: Supports both full sync and add-only operations.
        
        If users is provided:
        - If add_users_only=False (default): Full sync mode
          - New users (no user_id): Create user and add to list
          - Updated users (user_id exists): Update user data
          - Removed users (in current list but not in provided): Remove from list
        - If add_users_only=True: Add-only mode
          - New users (no user_id): Create user and add to list
          - Updated users (user_id exists): Update user data
          - Existing users not in provided list are NOT removed
        
        Args:
            list_id: ID of the list to edit
            list_name: Optional new name for the list
            is_active: Optional 1 to activate, 0 to deactivate
            users: Optional list of users to add/update. Behavior depends on add_users_only.
            add_users_only: If True, only add/update users without removing existing ones.
                           If False, users represents desired final state (full sync).
            
        Returns:
            Dictionary with operation results:
                {
                    'success': bool,
                    'list_id': int,
                    'list_name': Optional[str],
                    'is_active': Optional[int],
                    'users_added': int,
                    'users_updated': int,
                    'users_removed': int,
                    'errors': List[str]
                }
        """
        errors = []
        users_added = 0
        users_updated = 0
        users_removed = 0
        updated_list_name = list_name
        updated_is_active = is_active
        
        try:
            if not self.db._is_connected:
                self.db.connect()
            
            with self.db.get_connection() as conn:
                from sqlalchemy import text
                
                # Step 1: Update list name and/or is_active if provided
                if list_name is not None or is_active is not None:
                    update_fields = []
                    update_params = {'list_id': list_id}
                    
                    if list_name is not None:
                        update_fields.append(f'{self.list_name_field} = :{self.list_name_field}')
                        update_params[self.list_name_field] = list_name
                    
                    if is_active is not None:
                        update_fields.append(f'{self.list_active_field} = :{self.list_active_field}')
                        update_params[self.list_active_field] = is_active
                    
                    if update_fields:
                        update_query = f"""
                            UPDATE {self.lists_table}
                            SET {', '.join(update_fields)}
                            WHERE {self.list_primary_key} = :list_id
                        """
                        update_params['list_id'] = list_id
                        result = conn.execute(text(update_query), update_params)
                        conn.commit()
                        
                        # Verify update succeeded
                        if result.rowcount == 0:
                            errors.append(f"List with ID {list_id} not found")
                
                # Step 2: Handle users diff if provided
                if users is not None:
                    # Get current users in the list
                    current_users = self._get_current_users_in_list(conn, list_id)
                    current_user_ids = set(current_users.keys())
                    
                    # Extract user IDs from provided users (those with user_id or id field)
                    provided_user_ids = set()
                    new_users = []
                    updated_users = []
                    users_to_add_to_list = []  # Users that exist but need to be added to list
                    
                    for user in users:
                        user_id = user.get('user_id') or user.get('id') or user.get(self.user_primary_key)
                        if user_id:
                            provided_user_ids.add(user_id)
                            # Check if user is in current list
                            if user_id in current_users:
                                # User is in current list - check if data changed
                                current_user = current_users[user_id]
                                # Compare user data (excluding id fields)
                                exclude_fields = ['id', 'user_id', self.user_primary_key]
                                user_data_for_comparison = {k: v for k, v in user.items() if k not in exclude_fields}
                                current_data_for_comparison = {k: v for k, v in current_user.items() if k not in exclude_fields}
                                
                                if user_data_for_comparison != current_data_for_comparison:
                                    updated_users.append((user_id, user_data_for_comparison))
                            else:
                                # User ID provided but not in current list
                                # Check if user exists in database (we'll try to update/add to list)
                                exclude_fields = ['id', 'user_id', self.user_primary_key]
                                user_data_for_comparison = {k: v for k, v in user.items() if k not in exclude_fields}
                                updated_users.append((user_id, user_data_for_comparison))
                                users_to_add_to_list.append(user_id)
                        else:
                            # No user_id - this is a new user
                            new_users.append(user)
                    
                    # Determine removed users (in current but not in provided)
                    # Only calculate if not in add-only mode
                    removed_user_ids = set()
                    if not add_users_only:
                        removed_user_ids = current_user_ids - provided_user_ids
                    
                    # Step 2a: Create new users and add to list
                    for user_data in new_users:
                        new_user_id = self._create_user(conn, user_data)
                        if new_user_id:
                            users_added += 1
                            # Add new user to list
                            self._add_users_to_list(conn, list_id, [new_user_id])
                        else:
                            errors.append(f"Failed to create new user")
                    
                    # Step 2b: Update existing users
                    for user_id, user_data in updated_users:
                        if self._update_user(conn, user_id, user_data):
                            users_updated += 1
                        else:
                            # User might not exist - try to create it
                            # This handles case where user_id was provided but user doesn't exist
                            pass
                    
                    # Step 2b2: Add users to list that were updated but not in list
                    if users_to_add_to_list:
                        added_count = self._add_users_to_list(conn, list_id, users_to_add_to_list)
                        # Note: These are counted as updates, not new additions
                    
                    # Step 2c: Remove users from list (only in full sync mode)
                    if not add_users_only and removed_user_ids:
                        users_removed = self._remove_users_from_list(conn, list_id, list(removed_user_ids))
                    
                    conn.commit()
                
                # If list_name or is_active were not provided, fetch current values
                if list_name is None or is_active is None:
                    query = f"SELECT {self.list_name_field}, {self.list_active_field} FROM {self.lists_table} WHERE {self.list_primary_key} = :list_id"
                    result = conn.execute(text(query), {'list_id': list_id})
                    row = result.fetchone()
                    if row:
                        if list_name is None:
                            updated_list_name = row[0]
                        if is_active is None:
                            updated_is_active = row[1]
                    else:
                        errors.append(f"Could not fetch list information for ID {list_id}")
                
                success = len(errors) == 0
                
                return {
                    'success': success,
                    'list_id': list_id,
                    'list_name': updated_list_name,
                    'is_active': updated_is_active,
                    'users_added': users_added,
                    'users_updated': users_updated,
                    'users_removed': users_removed,
                    'errors': errors
                }
                
        except Exception as e:
            raise Exception(f"Failed to edit list: {str(e)}")
    
    def _add_users_to_list(
        self,
        conn,
        list_id: int,
        user_ids: List[int]
    ) -> int:
        """
        Add users to a list.
        
        Args:
            conn: Database connection
            list_id: ID of the list
            user_ids: List of user IDs to add
            
        Returns:
            Number of users successfully added
        """
        if not user_ids:
            return 0
        
        junction_table = self.users_to_lists_table
        
        # Use field names from config
        insert_query = f"""
            INSERT INTO {junction_table} ({self.foreign_key_list_id}, {self.foreign_key_user_id})
            VALUES (:{self.foreign_key_list_id}, :{self.foreign_key_user_id})
            ON DUPLICATE KEY UPDATE {self.foreign_key_list_id} = {self.foreign_key_list_id}
        """
        
        users_added = 0
        
        for user_id in user_ids:
            try:
                from sqlalchemy import text
                params = {self.foreign_key_list_id: list_id, self.foreign_key_user_id: user_id}
                result = conn.execute(text(insert_query), params)
                # Check if row was inserted (not updated)
                # ON DUPLICATE KEY UPDATE returns 2 for update, 1 for insert
                if result.rowcount > 0:
                    users_added += 1
            except Exception as e:
                print(f"Warning: Could not add user_id {user_id} to list_id {list_id}: {e}", file=sys.stderr)
        
        return users_added
    
    def _remove_users_from_list(
        self,
        conn,
        list_id: int,
        user_ids: List[int]
    ) -> int:
        """
        Remove users from a list.
        
        Args:
            conn: Database connection
            list_id: ID of the list
            user_ids: List of user IDs to remove
            
        Returns:
            Number of users successfully removed
        """
        if not user_ids:
            return 0
        
        junction_table = self.users_to_lists_table
        
        # Use field names from config
        delete_query = f"""
            DELETE FROM {junction_table}
            WHERE {self.foreign_key_list_id} = :list_id AND {self.foreign_key_user_id} = :user_id
        """
        
        users_removed = 0
        
        for user_id in user_ids:
            try:
                from sqlalchemy import text
                params = {self.foreign_key_list_id: list_id, self.foreign_key_user_id: user_id}
                result = conn.execute(text(delete_query), params)
                if result.rowcount > 0:
                    users_removed += 1
            except Exception as e:
                print(f"Warning: Could not remove user_id {user_id} from list_id {list_id}: {e}", file=sys.stderr)
        
        return users_removed
    
    def remove_list(self, list_id: int) -> Dict[str, Any]:
        """
        Remove (delete) a list and all its user associations.
        
        This method:
        1. Removes all user-list mappings from the junction table
        2. Deletes the list from the lists table
        
        Args:
            list_id: ID of the list to remove
            
        Returns:
            Dictionary with operation results:
                {
                    'success': bool,
                    'list_id': int,
                    'rows_affected': int,  # Total rows affected (junction + list)
                    'error': Optional[str],
                    'error_type': Optional[str]
                }
        """
        errors = []
        rows_affected = 0
        
        try:
            if not self.db._is_connected:
                self.db.connect()
            
            with self.db.get_connection() as conn:
                from sqlalchemy import text
                
                # Step 1: Remove all user-list mappings for this list
                junction_table = self.users_to_lists_table
                delete_junction_query = f"""
                    DELETE FROM {junction_table}
                    WHERE {self.foreign_key_list_id} = :list_id
                """
                
                try:
                    result = conn.execute(text(delete_junction_query), {self.foreign_key_list_id: list_id})
                    junction_rows_affected = result.rowcount
                    rows_affected += junction_rows_affected
                except Exception as e:
                    error_msg = f"Failed to remove user-list mappings: {str(e)}"
                    errors.append(error_msg)
                    print(f"Warning: {error_msg}", file=sys.stderr)
                
                # Step 2: Delete the list itself
                lists_table = self.lists_table
                delete_list_query = f"""
                    DELETE FROM {lists_table}
                    WHERE {self.list_primary_key} = :list_id
                """
                
                try:
                    result = conn.execute(text(delete_list_query), {'list_id': list_id})
                    list_rows_affected = result.rowcount
                    rows_affected += list_rows_affected
                    
                    if list_rows_affected == 0:
                        errors.append(f"List with ID {list_id} not found")
                except Exception as e:
                    error_msg = f"Failed to delete list: {str(e)}"
                    errors.append(error_msg)
                    print(f"Warning: {error_msg}", file=sys.stderr)
                
                # Commit if no errors, otherwise rollback
                if errors:
                    conn.rollback()
                else:
                    conn.commit()
                
                success = len(errors) == 0 and rows_affected > 0
                
                return {
                    'success': success,
                    'list_id': list_id,
                    'rows_affected': rows_affected,
                    'error': '; '.join(errors) if errors else None,
                    'error_type': 'OperationError' if errors else None
                }
                
        except Exception as e:
            error_msg = f"Error removing list: {str(e)}"
            print(f"✗ {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'list_id': list_id,
                'rows_affected': rows_affected,
                'error': error_msg,
                'error_type': type(e).__name__
            }
    
    def get_all_lists_with_users(self) -> List[Dict[str, Any]]:
        """
        Fetch all lists with their associated users.
        
        Returns:
            List of dictionaries, each containing:
            {
                'id': int,
                'list_name': str,
                'is_active': int,
                'created_at': str,
                'time_activate_modify': str,
                'users': List[Dict[str, Any]]  # Array of user dictionaries
            }
        
        Raises:
            Exception: If query fails
        """
        try:
            if not self.db._is_connected:
                self.db.connect()
            
            # Get table names from config
            lists_table = self.lists_table  # e.g., 'list_special_users'
            junction_table = self.users_to_lists_table  # e.g., 'special_users_list_mapping'
            
            # Get users table name from config
            # Navigate: data_base_tables -> special_users -> users -> table_name
            users_config = self.data_base_tables.get('users', {})
            users_table = users_config.get('table_name', 'special_users')
            
            # Query to fetch all lists with their users
            # Join lists table with junction table and users table
            # Use field names from config
            query = f"""
                SELECT 
                    l.{self.list_primary_key} as list_id,
                    l.{self.list_name_field} as list_name,
                    l.{self.list_active_field} as is_active,
                    l.created_at,
                    l.time_activate_modify,
                    u.{self.user_primary_key} as user_id,
                    u.*
                FROM {lists_table} l
                LEFT JOIN {junction_table} j ON l.{self.list_primary_key} = j.{self.foreign_key_list_id}
                LEFT JOIN {users_table} u ON j.{self.foreign_key_user_id} = u.{self.user_primary_key}
                ORDER BY l.{self.list_primary_key}, u.{self.user_primary_key}
            """
            
            with self.db.get_connection() as conn:
                from sqlalchemy import text
                result = conn.execute(text(query))
                rows = result.fetchall()
                
                if not rows:
                    return []
                
                # Get column names
                columns = result.keys()
                
                # Group rows by list_id
                lists_dict = {}
                # List columns that belong to the list table
                list_columns = ['list_id', 'list_name', 'is_active', 'created_at', 'time_activate_modify']
                # User columns are all columns except list columns and user_id (which is duplicate of u.{user_primary_key})
                user_columns = [col for col in columns if col not in list_columns and col != 'user_id']
                
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    list_id = row_dict['list_id']
                    
                    # Initialize list entry if not exists
                    if list_id not in lists_dict:
                        lists_dict[list_id] = {
                            'id': list_id,
                            'list_name': row_dict['list_name'],
                            'is_active': row_dict['is_active'],
                            'created_at': str(row_dict['created_at']) if row_dict['created_at'] else None,
                            'time_activate_modify': str(row_dict['time_activate_modify']) if row_dict['time_activate_modify'] else None,
                            'users': []
                        }
                    
                    # Add user if user_id exists (not NULL)
                    user_id = row_dict.get('user_id')  # User's primary key from the join
                    if user_id is not None:
                        # Build user dictionary with all user fields
                        user_dict = {}
                        for col in user_columns:
                            value = row_dict.get(col)
                            # Convert datetime/timestamp to string
                            if value is not None:
                                if hasattr(value, 'isoformat'):
                                    user_dict[col] = value.isoformat()
                                else:
                                    user_dict[col] = value
                            else:
                                user_dict[col] = None
                        
                        # Only add user if not already added (avoid duplicates)
                        # Check using user's primary key field
                        user_primary_key_value = user_dict.get(self.user_primary_key) or user_id
                        if not any(u.get(self.user_primary_key) == user_primary_key_value or u.get('id') == user_primary_key_value for u in lists_dict[list_id]['users']):
                            lists_dict[list_id]['users'].append(user_dict)
                
                # Convert to list
                return list(lists_dict.values())
                
        except Exception as e:
            raise Exception(f"Failed to fetch lists with users: {str(e)}")



