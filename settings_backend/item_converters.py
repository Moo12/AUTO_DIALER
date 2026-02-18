"""
Item Converter Classes

Base class and derived classes for converting request data to database-ready format.
"""

import re
from typing import Dict, Any, Optional
from common_utils.config_manager import ConfigManager
import sys

class BaseItemConverter:
    """
    Base class for item converters.
    Returns None by default (no conversion).
    Derived classes should implement convert() method.
    """
    
    @staticmethod
    def convert(
        request_data: Dict[str, Any], 
        item_type: str, 
        config_manager: ConfigManager
    ) -> Optional[Dict[str, Any]]:
        """
        Convert request data to database-ready format.
        
        Args:
            request_data: Dictionary from request
            item_type: Type key that matches data_base_tables configuration
            config_manager: ConfigManager instance
        
        Returns:
            Converted dictionary or None if no conversion needed
        """
        return None


class SpamPatternsConverter(BaseItemConverter):
    """Converter for spam_patterns item type."""
    
    @staticmethod
    def _create_regex_from_range(start_range: str, end_range: str) -> str:
        """
        Create a regex pattern from start and end range.
        
        Args:
            start_range: Start range string (e.g., "0341XXXXXX")
            end_range: End range string (e.g., "0344XXXXXX")
        
        Returns:
            Regex pattern string (e.g., "034[1-4][0-9]{6}")
        
        Example:
            start_range="0341XXXXXX", end_range="0344XXXXXX" 
            -> "034[1-4][0-9]{6}"
        """
        # Normalize lengths by padding shorter string with 'X' at the end
        max_len = max(len(start_range), len(end_range))
        start_range = start_range.ljust(max_len, 'X')
        end_range = end_range.ljust(max_len, 'X')
        
        # Find common prefix (characters that are the same)
        common_prefix = ""
        for i in range(max_len):
            if start_range[i] == end_range[i]:
                common_prefix += start_range[i]
            else:
                break
        
        # Get the differing parts
        start_suffix = start_range[len(common_prefix):]
        end_suffix = end_range[len(common_prefix):]
        
        # Build regex pattern
        pattern_parts = []
        
        # Add common prefix (replace X with [0-9], keep digits as-is, escape other chars)
        if common_prefix:
            escaped_prefix = ""
            for char in common_prefix:
                if char == 'X':
                    escaped_prefix += '[0-9]'
                elif char.isdigit():
                    escaped_prefix += char
                else:
                    escaped_prefix += re.escape(char)
            pattern_parts.append(escaped_prefix)
        
        # Process the differing suffix character by character
        i = 0
        while i < len(start_suffix):
            start_char = start_suffix[i]
            end_char = end_suffix[i]
            
            if start_char == 'X' and end_char == 'X':
                # Both are X, match any digit
                pattern_parts.append('[0-9]')
            elif start_char == 'X' and end_char.isdigit():
                # Start is X, end is digit - match 0 to end_digit
                pattern_parts.append(f'[0-{end_char}]')
            elif start_char.isdigit() and end_char == 'X':
                # Start is digit, end is X - match start_digit to 9
                pattern_parts.append(f'[{start_char}-9]')
            elif start_char == end_char:
                # Same character
                if start_char == 'X':
                    pattern_parts.append('[0-9]')
                elif start_char.isdigit():
                    pattern_parts.append(start_char)
                else:
                    pattern_parts.append(re.escape(start_char))
            elif start_char.isdigit() and end_char.isdigit():
                # Different digits - create range
                start_digit = int(start_char)
                end_digit = int(end_char)
                if start_digit <= end_digit:
                    pattern_parts.append(f'[{start_char}-{end_char}]')
                else:
                    # Invalid range, match any digit
                    pattern_parts.append('[0-9]')
            else:
                # Non-digit characters, match literally
                pattern_parts.append(re.escape(start_char))
            
            i += 1
        
        # Combine pattern parts
        regex_pattern = ''.join(pattern_parts)
        
        # Optimize: Replace consecutive [0-9] patterns with {n}
        # Count consecutive [0-9] patterns
        optimized_pattern = ""
        i = 0
        while i < len(regex_pattern):
            if regex_pattern[i:i+5] == '[0-9]':
                count = 0
                j = i
                while j < len(regex_pattern) and regex_pattern[j:j+5] == '[0-9]':
                    count += 1
                    j += 5
                if count > 1:
                    optimized_pattern += f'[0-9]{{{count}}}'
                else:
                    optimized_pattern += '[0-9]'
                i = j
            else:
                optimized_pattern += regex_pattern[i]
                i += 1

        print(f"optimized_pattern: {optimized_pattern}", file=sys.stderr)
        
        return optimized_pattern
    
    @staticmethod
    def convert(
        request_data: Dict[str, Any], 
        item_type: str, 
        config_manager: ConfigManager
    ) -> Dict[str, Any]:
        """Convert spam_patterns request data."""
        converted = request_data.copy()

        start_range = request_data.get('range_start')
        end_range = request_data.get('range_end')
        
        # Also check alternative field names
        if start_range is None:
            start_range = request_data.get('start_range')
        if end_range is None:
            end_range = request_data.get('end_range')

        if start_range is not None and end_range is not None:
            # Create regex pattern from range
            regex_pattern = SpamPatternsConverter._create_regex_from_range(
                str(start_range), 
                str(end_range)
            )
            # Set the pattern_regex field
            converted['pattern_regex'] = regex_pattern
            # Remove range fields if they shouldn't be stored
            # (uncomment if you want to remove them)
            # converted.pop('range_start', None)
            # converted.pop('range_end', None)
            # converted.pop('start_range', None)
            # converted.pop('end_range', None)

        return converted


class CompetingCompanyConfigsConverter(BaseItemConverter):
    """Converter for competing_company_configs item type."""
    
    @staticmethod
    def convert(
        request_data: Dict[str, Any], 
        item_type: str, 
        config_manager: ConfigManager
    ) -> Optional[Dict[str, Any]]:
        """Convert competing_company_configs request data."""
        # Return None to use default behavior (no conversion)
        return None

