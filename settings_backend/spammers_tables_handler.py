"""
Spammers Tables Handler Module

Checks phone numbers against spam sources and returns combined mail titles/subtitles.
Each source type has its own class that implements matching logic.
"""

import sys
import re
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from common_utils.db_connection import DatabaseConnection


class BaseSource(ABC):
    """
    Abstract base class for all spam source types.
    Each source checks if a phone number matches its conditions.
    """
    
    def __init__(self, db_connection: DatabaseConnection, config: Dict[str, Any]):
        """
        Initialize base source.
        
        Args:
            db_connection: DatabaseConnection instance
            config: Source-specific configuration dictionary
        """
        self.db = db_connection
        self.config = config
        self.source_type = config.get('source_type', '')
        self._cached_match_info: Optional[Dict[str, Any]] = None
        self._cached_phone_number: Optional[str] = None
        self._cached_kwargs: Optional[Dict[str, Any]] = None
    
    @abstractmethod
    def matches(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Check if phone number matches this source's conditions and return match info.
        
        This method should perform a single database query and return all necessary
        information (mail_title, mail_subtitle, source_id, etc.) if a match is found.
        
        Args:
            phone_number: Phone number to check
            **kwargs: Additional parameters (e.g., company_id for company source)
        
        Returns:
            Dictionary with match info if phone matches, None otherwise.
            Should include at least 'mail_title' and 'mail_subtitle' keys.
        """
        pass
    
    def get_mail_title(self, phone_number: str, **kwargs) -> Optional[str]:
        """
        Get mail title if phone matches.
        Uses cached match info from matches() to avoid redundant DB calls.
        
        Args:
            phone_number: Phone number to check
            **kwargs: Additional parameters
        
        Returns:
            Mail title string if match found, None otherwise
        """
        match_info = self._get_cached_match_info(phone_number, **kwargs)
        return match_info.get('mail_title') if match_info else None
    
    def get_mail_subtitle(self, phone_number: str, **kwargs) -> Optional[str]:
        """
        Get mail subtitle if phone matches.
        Uses cached match info from matches() to avoid redundant DB calls.
        
        Args:
            phone_number: Phone number to check
            **kwargs: Additional parameters
        
        Returns:
            Mail subtitle string if match found, None otherwise
        """
        match_info = self._get_cached_match_info(phone_number, **kwargs)
        return match_info.get('mail_subtitle') if match_info else None
    
    def _get_cached_match_info(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Get cached match info or fetch it if not cached or parameters changed.
        
        Args:
            phone_number: Phone number to check
            **kwargs: Additional parameters
        
        Returns:
            Dictionary with match info if match found, None otherwise
        """
        # Check if cache is valid (same phone number and kwargs)
        if (self._cached_match_info is not None and 
            self._cached_phone_number == phone_number and
            self._cached_kwargs == kwargs):
            return self._cached_match_info
        
        # Fetch match info and cache it
        match_info = self.matches(phone_number, **kwargs)
        self._cached_match_info = match_info
        self._cached_phone_number = phone_number
        self._cached_kwargs = kwargs
        
        return match_info
    
    def get_match_info(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Get full match information (title, subtitle, source_id, etc.).
        Uses cached match info from matches() to avoid redundant DB calls.
        
        Args:
            phone_number: Phone number to check
            **kwargs: Additional parameters
        
        Returns:
            Dictionary with match info if match found, None otherwise
        """
        match_info = self._get_cached_match_info(phone_number, **kwargs)
        if match_info:
            match_info['source_type'] = self.source_type
        return match_info


class CompanySource(BaseSource):
    """
    Source for matching phone numbers against company configurations.
    Requires company_id to determine which company config to check.
    """
    
    def __init__(self, db_connection: DatabaseConnection, config: Dict[str, Any]):
        super().__init__(db_connection, config)
        self.table_name = config.get('table_name', 'competing_company_configs')
        self.company_id_field = config.get('company_id_field', 'company_id')
        self.mail_title_field = config.get('mail_title_field', 'mail_title')
        self.mail_subtitle_field = config.get('mail_subtitle_field', 'mail_subtitle')
        self.active_field = config.get('active_field', 'is_active')
    
    def matches(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Check if phone number matches company config and return match info.
        
        Args:
            phone_number: Phone number to check
            **kwargs: Additional parameters, must include 'company_id' (required)
        
        Returns:
            Dictionary with mail_title and mail_subtitle if active company config exists,
            None otherwise
        """
        company_id = kwargs.get('company_id')
        if company_id is None:
            return None
        
        try:
            id_field = self.config.get('id_field', 'id')
            query = f"""
                SELECT `{id_field}`, `{self.mail_title_field}`, `{self.mail_subtitle_field}`
                FROM `{self.table_name}`
                WHERE `{self.company_id_field}` = :company_id
                AND `{self.active_field}` = 1
                LIMIT 1
            """
            result = self.db.execute_query(query, {'company_id': company_id})
            if result and len(result) > 0:
                row = result[0]
                return {
                    'source_id': row.get(id_field),
                    'mail_title': row.get(self.mail_title_field),
                    'mail_subtitle': row.get(self.mail_subtitle_field)
                }
        except Exception as e:
            print(f"⚠️  Warning: Error checking company source: {e}", file=sys.stderr)
        return None

class PatternSource(BaseSource):
    """
    Source for matching phone numbers against regex patterns.
    """
    
    def __init__(self, db_connection: DatabaseConnection, config: Dict[str, Any]):
        super().__init__(db_connection, config)
        self.table_name = config.get('table_name', 'spam_patterns')
        self.mail_title_field = config.get('mail_title_field', 'mail_title')
        self.mail_subtitle_field = config.get('sub_mail_title_field', 'sub_mail_title')
        self.pattern_regex_field = config.get('pattern_regex_field', 'pattern_regex')
        self.range_start_field = config.get('range_start_field', 'range_start')
        self.range_end_field = config.get('range_end_field', 'range_end')
        self.active_field = config.get('active_field', 'is_active')
    
    def matches(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Check if phone number matches any active pattern and return match info.
        
        Returns match info from the first matching pattern found.
        """
        try:
            id_field = self.config.get('id_field', 'id')
            query = f"""
                SELECT `{id_field}`, `{self.mail_title_field}`, `{self.mail_subtitle_field}`, 
                       `{self.pattern_regex_field}`, `{self.range_start_field}`, `{self.range_end_field}`
                FROM `{self.table_name}`
                WHERE `{self.active_field}` = 1
                ORDER BY {id_field} ASC
            """
            patterns = self.db.execute_query(query)
            
            for pattern_row in patterns:
                pattern_regex = pattern_row.get(self.pattern_regex_field)
                range_start = pattern_row.get(self.range_start_field)
                range_end = pattern_row.get(self.range_end_field)
                mail_title = pattern_row.get(self.mail_title_field)
                mail_subtitle = pattern_row.get(self.mail_subtitle_field)
                source_id = pattern_row.get(id_field)
                
                # Check if matches
                matches = False

                if pattern_regex:
                    try:
                        matches = bool(re.match(pattern_regex, phone_number))
                    except re.error:
                        print(f"⚠️  Warning: Invalid regex pattern: {pattern_regex}", file=sys.stderr)
                        continue
                elif range_start and range_end:
                    matches = range_start <= phone_number <= range_end
                
                if matches:
                    return {
                        'source_id': source_id,
                        'mail_title': mail_title,
                        'mail_subtitle': mail_subtitle
                    }
            
            return None
        except Exception as e:
            print(f"⚠️  Warning: Error checking pattern source: {e}", file=sys.stderr)
            return None


class SpecialUsersSource(BaseSource):
    """
    Source for matching phone numbers against special users lists.
    """
    
    def __init__(self, db_connection: DatabaseConnection, config: Dict[str, Any]):
        super().__init__(db_connection, config)
        self.users_table = config.get('users_table', 'special_users')
        self.lists_table = config.get('lists_table', 'list_special_users')
        self.mapping_table = config.get('mapping_table', 'special_users_list_mapping')
        self.mail_title_field = config.get('mail_title_field', 'email_title')
        self.mail_subtitle_field = config.get('mail_subtitle_field', 'email_sub_title')
        self.phone_field = config.get('phone_field', 'phone_number')
        self.active_field = config.get('active_field', 'is_active')
    
    def matches(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Check if phone number exists in special users and return match info.
        """
        try:
            user_id_field = self.config.get('user_id_field', 'id')
            query = f"""
                SELECT su.`{user_id_field}`, su.`{self.mail_title_field}`, su.`{self.mail_subtitle_field}`
                FROM `{self.users_table}` su
                INNER JOIN `{self.mapping_table}` sulm ON su.{user_id_field} = sulm.user_id
                INNER JOIN `{self.lists_table}` sul ON sul.id = sulm.list_id
                WHERE su.`{self.phone_field}` = :phone_number
                AND sul.`{self.active_field}` = 1
                LIMIT 1
            """
            result = self.db.execute_query(query, {'phone_number': phone_number})
            if result and len(result) > 0:
                row = result[0]
                return {
                    'source_id': row.get(user_id_field),
                    'mail_title': row.get(self.mail_title_field),
                    'mail_subtitle': row.get(self.mail_subtitle_field)
                }
        except Exception as e:
            print(f"⚠️  Warning: Error checking special users source: {e}", file=sys.stderr)
        return None


class MainPhoneTableSource(BaseSource):
    """
    Source for matching phone numbers directly in phone_numbers table.
    Returns phone_name for both title and subtitle.
    """
    
    def __init__(self, db_connection: DatabaseConnection, config: Dict[str, Any]):
        super().__init__(db_connection, config)
        self.table_name = config.get('table_name', 'phone_numbers')
        self.phone_field = config.get('phone_field', 'phone_number')
        self.phone_name_field = config.get('phone_name_field', 'phone_name')
    
    def matches(self, phone_number: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Check if phone number exists in phone_numbers table and return match info.
        Returns phone_name for both title and subtitle.
        """
        try:
            id_field = self.config.get('id_field', 'phone_id')
            query = f"""
                SELECT `{id_field}`, `{self.phone_name_field}`
                FROM `{self.table_name}`
                WHERE `{self.phone_field}` = :phone_number
                LIMIT 1
            """
            result = self.db.execute_query(query, {'phone_number': phone_number})
            if result and len(result) > 0:
                phone_name = result[0].get(self.phone_name_field)
                source_id = result[0].get(id_field)
                if phone_name:
                    return {
                        'source_id': source_id,
                        'mail_title': phone_name,
                        'mail_subtitle': phone_name
                    }
        except Exception as e:
            print(f"⚠️  Warning: Error checking main phone table source: {e}", file=sys.stderr)
        return None


class SpammersTablesHandler:
    """
    Main handler that checks phone numbers against all spam sources.
    Returns combined mail title and subtitle based on source priority order.
    """
    
    def __init__(self, db_connection: DatabaseConnection, config: Dict[str, Any]):
        """
        Initialize handler with database connection and source configurations.
        
        Args:
            db_connection: DatabaseConnection instance
            config: Dictionary containing:
                - 'spam_sources': Dict with:
                    - 'source_priority_order': List[str] - Order of sources to check
                    - 'source_type_mapping': Dict[str, str] - Maps source type to data_base_tables key
                - 'data_base_tables': Dict[str, Dict] - Table configurations (field definitions)
        """
        self.db = db_connection
        
        # Get spam_sources config
        spam_sources_config = config.get('spam_sources', {})
        self.source_priority_order = spam_sources_config.get('source_priority_order', [
            'special_users', 'pattern', 'company', 'main_phone_table'
        ])
        source_type_mapping = spam_sources_config.get('source_type_mapping', {})
        
        # Get data_base_tables config
        self.data_base_tables = config.get('data_base_tables', {})
        
        # Initialize source instances
        self.sources = {}
        source_classes = {
            'company': CompanySource,
            'pattern': PatternSource,
            'special_users': SpecialUsersSource,
            'main_phone_table': MainPhoneTableSource
        }
        
        for source_type, db_table_key in source_type_mapping.items():
            # Check if source class exists
            if source_type not in source_classes:
                print(f"⚠️  Warning: Source type '{source_type}' not found in source_classes, skipping", file=sys.stderr)
                continue
            
            # Check if data_base_tables key exists
            if db_table_key not in self.data_base_tables:
                print(f"⚠️  Warning: Data base table key '{db_table_key}' not found in data_base_tables, skipping", file=sys.stderr)
                continue
            
            source_class = source_classes[source_type]
            
            # Get the table config from data_base_tables
            table_config = self.data_base_tables[db_table_key]
            
            # Build source config based on source type
            source_config = self._build_source_config(source_type, table_config, self.data_base_tables)
            if source_config:
                source_config['source_type'] = source_type
                self.sources[source_type] = source_class(db_connection, source_config)
    
    def _build_source_config(
        self, 
        source_type: str, 
        table_config: Dict[str, Any],
        data_base_tables: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Build source-specific config from data_base_tables entry.
        
        Args:
            source_type: Type of source ('company', 'pattern', 'special_users', 'main_phone_table')
            table_config: Configuration from data_base_tables for this source
            data_base_tables: Full data_base_tables dictionary
        
        Returns:
            Source configuration dictionary or None if invalid
        """
        if source_type == 'company':
            return {
                'table_name': table_config.get('table_name'),
                'id_field': table_config.get('id_field', 'id'),
                'company_id_field': table_config.get('company_id_field', 'company_id'),
                'mail_title_field': table_config.get('mail_title_field', 'mail_title'),
                'mail_subtitle_field': table_config.get('mail_subtitle_field', 'mail_subtitle'),
                'active_field': table_config.get('active_field', 'is_active')
            }
        
        elif source_type == 'pattern':
            return {
                'table_name': table_config.get('table_name'),
                'id_field': table_config.get('id_field', 'id'),
                'mail_title_field': table_config.get('mail_title_field', 'mail_title'),
                'sub_mail_title_field': table_config.get('sub_mail_title_field', 'sub_mail_title'),
                'pattern_regex_field': table_config.get('pattern_regex_field', 'pattern_regex'),
                'range_start_field': table_config.get('range_start_field', 'range_start'),
                'range_end_field': table_config.get('range_end_field', 'range_end'),
                'active_field': table_config.get('active_field', 'is_active')
            }
        
        elif source_type == 'special_users':
            # special_users has nested structure: users, lists, users_to_lists
            users_config = table_config.get('users', {})
            lists_config = table_config.get('lists', {})
            return {
                'users_table': users_config.get('table_name', 'special_users'),
                'lists_table': lists_config.get('table_name', 'list_special_users'),
                'mapping_table': table_config.get('users_to_lists', {}).get('table_name', 'special_users_list_mapping'),
                'user_id_field': users_config.get('primary_key', 'id'),
                'mail_title_field': users_config.get('mail_title_field', 'email_title'),
                'mail_subtitle_field': users_config.get('mail_subtitle_field', 'email_sub_title'),
                'phone_field': users_config.get('phone_field', 'phone_number'),
                'active_field': lists_config.get('active_field', 'is_active')
            }
        
        elif source_type == 'main_phone_table':
            return {
                'table_name': table_config.get('table_name'),
                'id_field': table_config.get('primary_key', 'phone_id'),
                'phone_field': table_config.get('phone_field', 'phone_number'),
                'phone_name_field': table_config.get('phone_name_field', 'phone_name')
            }
        
        return None
    
    def _get_company_id_from_phone_profile(
        self, 
        phone_number: str
    ) -> Optional[int]:
        """
        Get company_id from phone_profile id.
        
        Flow:
        1. Get network_company from phone_profiles table using phone_profile_id
        2. Get company_id from competing_networks table using network_company value
        
        Args:
            phone_profile_id: ID of the phone profile record
        
        Returns:
            company_id if found, None otherwise
        """
        try:
            # Get phone_profiles config
            phone_profiles_config = self.data_base_tables.get('phone_profiles', {})
            phone_profiles_table = phone_profiles_config.get('table_name', 'phone_profiles')
            network_company_field = phone_profiles_config.get('network_company_field', 'network_company')
            phone_number_fields = phone_profiles_config.get('phone_number_field', 'phone_number')
            
            # Step 1: Get network_company from phone_profiles
            query1 = f"""

                SELECT `{network_company_field}`
                FROM `{phone_profiles_table}`
                WHERE `{phone_number_fields}` = :phone_number
                LIMIT 1
            """
            result1 = self.db.execute_query(query1, {'phone_number': phone_number})
            
            if not result1 or len(result1) == 0:
                return None
            
            network_company = result1[0].get(network_company_field)

            if network_company is None:
                print(f"⚠️  Warning: network_company is None for phone_number {phone_number}", file=sys.stderr)
                return None
            
            # Step 2: Get company_id from competing_networks using network_company
            competing_networks_config = self.data_base_tables.get('competing_networks', {})
            competing_networks_table = competing_networks_config.get('table_name', 'competing_networks')
            competing_networks_primary_key = competing_networks_config.get('primary_key', 'id')
            
            # Get the field name to match on from config (default to 'network_company' or 'name')
            network_field_name = competing_networks_config.get('network_field_name', 'network_company')
            # Fallback to 'name' if network_field_name not found and network_company doesn't exist
            query2 = f"""
                SELECT `{competing_networks_primary_key}`
                FROM `{competing_networks_table}`
                WHERE `{network_field_name}` = :network_company
                LIMIT 1
            """
            result2 = self.db.execute_query(query2, {'network_company': network_company})
            
            if result2 and len(result2) > 0:
                company_id = result2[0].get(competing_networks_primary_key)
                return company_id
            
            print(f"⚠️  Warning: Company not found for network_company '{network_company}'", file=sys.stderr)
            return None
            
        except Exception as e:
            print(f"⚠️  Warning: Error getting company_id from phone_profile: {e}", file=sys.stderr)
            return None
    
    def check_phone_number(
        self, 
        phone_number: str, 
        additional_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Check phone number against all sources and return combined mail titles.
        
        Args:
            phone_number: Phone number to check
            additional_params: Optional dictionary with additional parameters for source matching
                             (e.g., {"company_id": 1} for company source matching)
        
        Returns:
            {
                'mail_title': str,  # Concatenated title from all matching sources
                'mail_subtitle': str,  # Concatenated subtitle from all matching sources
                'matched_sources': List[Dict],  # List of matching sources with their info
                'success': bool
            }
        """

        matched_sources = []
        titles = []
        subtitles = []

        # Prepare kwargs from additional_params
        kwargs = additional_params.copy() if additional_params else {}
        
        # If company_id is not provided, try to derive it from phone_profile_id if available
        if 'company_id' not in kwargs:
            company_id = self._get_company_id_from_phone_profile(phone_number)
            if company_id:
                kwargs['company_id'] = company_id
        
        # Check each source in priority order
        for source_type in self.source_priority_order:
            if source_type not in self.sources:
                continue

            source = self.sources[source_type]
            
            match_info = source.get_match_info(phone_number, **kwargs)


            if match_info:
                matched_sources.append({
                    'source_type': source_type,
                    'source_id': match_info.get('source_id'),
                    'mail_title': match_info.get('mail_title'),
                    'mail_subtitle': match_info.get('mail_subtitle')
                })
                
                # Collect titles and subtitles (filter out None/empty)
                if match_info.get('mail_title'):
                    titles.append(match_info['mail_title'])
                if match_info.get('mail_subtitle'):
                    subtitles.append(match_info['mail_subtitle'])
        
        # Concatenate titles and subtitles
        combined_title = '\n'.join(titles) if titles else None
        combined_subtitle = '\n'.join(subtitles) if subtitles else None
        
        return {
            'mail_title': combined_title,
            'mail_subtitle': combined_subtitle,
            'matched_sources': matched_sources,
            'success': True
        }
    
    def get_detection_sources(
        self, 
        phone_number: str, 
        additional_params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all detection sources that match a phone number.
        
        Args:
            phone_number: Phone number to check
            additional_params: Optional dictionary with additional parameters for source matching
                             (e.g., {"company_id": 1} for company source matching)
        
        Returns:
            List of dictionaries, each containing:
            {
                'source_name': str,  # Source type name (e.g., 'pattern', 'company', etc.)
                'source_id': Any  # ID of the matching source record
            }
        """
        detection_sources = []
        
        # Prepare kwargs from additional_params
        kwargs = additional_params.copy() if additional_params else {}

        if 'company_id' not in kwargs:
            company_id = self._get_company_id_from_phone_profile(phone_number)
            if company_id:
                kwargs['company_id'] = company_id
        
        # Check each source in priority order
        for source_type in self.source_priority_order:
            if source_type not in self.sources:
                continue
            
            source = self.sources[source_type]
            
            match_info = source.get_match_info(phone_number, **kwargs)
            if match_info and match_info.get('source_id') is not None:
                detection_sources.append({
                    'source_name': source_type,
                    'source_id': match_info.get('source_id'),
                })
        
        return detection_sources
