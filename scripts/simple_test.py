"""
Enhanced test file for comprehensive pipeline testing
This file is designed to be large enough to trigger the splitting logic
"""

def add_numbers(a, b):
    """Add two numbers and return the result""" 
    return a + b 
#test
#test_12
#temp 
def subtract_numbers(a, b):
    """Subtract second number from first"""
    return a - b
#test
def greet_user(name):
    """Greet a user by name"""
    print(f"Hello, {name}!")
    return f"Greeting sent to {name}"

def calculate_area(length, width):
    """Calculate area of a rectangle"""
    if length <= 0 or width <= 0:
        raise ValueError("Length and width must be positive")
    return length * width

class Calculator:
    """Simple calculator class"""
    
    def __init__(self):
        self.history = []
    
    def add(self, a, b):
        result = a + b
        self.history.append(f"{a} + {b} = {result}")
        return result
    
    def multiply(self, a, b):
        result = a * b
        self.history.append(f"{a} * {b} = {result}")
        return result

# Adding many more functions to increase file size
def data_processor_function_1(data_input):
    """Process data using method 1"""
    processed_data = []
    for item in data_input:
        if isinstance(item, str):
            processed_item = item.upper().strip()
            if len(processed_item) > 0:
                processed_data.append(processed_item)
        elif isinstance(item, (int, float)):
            processed_item = item * 2
            processed_data.append(processed_item)
        else:
            processed_data.append(str(item))
    return processed_data

def data_processor_function_2(data_input):
    """Process data using method 2"""
    result = {}
    counter = 0
    for index, item in enumerate(data_input):
        if item is not None:
            key = f"item_{counter}"
            result[key] = {
                'original_index': index,
                'value': item,
                'processed': True,
                'timestamp': 'placeholder_timestamp'
            }
            counter += 1
    return result

def data_validator_function_1(data):
    """Validate data structure and content"""
    validation_results = {
        'is_valid': True,
        'errors': [],
        'warnings': []
    }
    
    if not isinstance(data, (list, dict, tuple)):
        validation_results['is_valid'] = False
        validation_results['errors'].append('Data must be a collection type')
        return validation_results
    
    if len(data) == 0:
        validation_results['warnings'].append('Data collection is empty')
    
    # Additional validation logic
    for item in data:
        if item is None:
            validation_results['warnings'].append('Found null item in data')
    
    return validation_results

def string_manipulation_function_1(input_string):
    """Perform various string manipulations"""
    if not isinstance(input_string, str):
        return None
    
    # Multiple string operations
    result = input_string.strip()
    result = result.replace('  ', ' ')
    result = result.replace('\t', ' ')
    result = result.replace('\n', ' ')
    
    # Word processing
    words = result.split(' ')
    processed_words = []
    for word in words:
        if len(word) > 0:
            processed_word = word.lower()
            if processed_word.isalpha():
                processed_words.append(processed_word)
    
    return ' '.join(processed_words)

def string_manipulation_function_2(input_string):
    """Another string manipulation function"""
    if not input_string:
        return ""
    
    # Character-by-character processing
    result_chars = []
    for char in input_string:
        if char.isalnum():
            result_chars.append(char.upper())
        elif char.isspace():
            result_chars.append('_')
        else:
            result_chars.append('#')
    
    return ''.join(result_chars)

def mathematical_operations_function_1(numbers):
    """Perform mathematical operations on a list of numbers"""
    if not numbers or len(numbers) == 0:
        return {'error': 'Empty number list provided'}
    
    try:
        total_sum = sum(numbers)
        average = total_sum / len(numbers)
        maximum = max(numbers)
        minimum = min(numbers)
        
        # Calculate variance
        variance = sum((x - average) ** 2 for x in numbers) / len(numbers)
        
        # Calculate median
        sorted_numbers = sorted(numbers)
        n = len(sorted_numbers)
        if n % 2 == 0:
            median = (sorted_numbers[n//2 - 1] + sorted_numbers[n//2]) / 2
        else:
            median = sorted_numbers[n//2]
        
        return {
            'sum': total_sum,
            'average': average,
            'maximum': maximum,
            'minimum': minimum,
            'variance': variance,
            'median': median,
            'count': len(numbers)
        }
    except Exception as e:
        return {'error': f'Calculation error: {str(e)}'}

def file_operations_function_1(filename):
    """Simulate file operations"""
    operations_log = []
    
    try:
        # Simulate checking if file exists
        operations_log.append(f"Checking if {filename} exists")
        
        # Simulate reading file
        operations_log.append(f"Attempting to read {filename}")
        simulated_content = "This is simulated file content"
        
        # Simulate processing content
        operations_log.append("Processing file content")
        processed_lines = simulated_content.split('\n')
        
        # Simulate validation
        operations_log.append("Validating processed content")
        valid_lines = [line for line in processed_lines if len(line.strip()) > 0]
        
        return {
            'success': True,
            'operations_log': operations_log,
            'processed_lines': valid_lines,
            'line_count': len(valid_lines)
        }
        
    except Exception as e:
        operations_log.append(f"Error occurred: {str(e)}")
        return {
            'success': False,
            'operations_log': operations_log,
            'error': str(e)
        }

def database_simulation_function_1(query_type, parameters):
    """Simulate database operations"""
    if query_type == "SELECT":
        return simulate_select_query(parameters)
    elif query_type == "INSERT":
        return simulate_insert_query(parameters)
    elif query_type == "UPDATE":
        return simulate_update_query(parameters)
    elif query_type == "DELETE":
        return simulate_delete_query(parameters)
    else:
        return {"error": "Unsupported query type"}

def simulate_select_query(parameters):
    """Simulate a SELECT database query"""
    simulated_results = []
    table_name = parameters.get('table', 'unknown_table')
    
    # Simulate returning some results
    for i in range(5):
        record = {
            'id': i + 1,
            'name': f'Record_{i + 1}',
            'value': (i + 1) * 10,
            'created_at': f'2024-01-{(i % 28) + 1:02d}'
        }
        simulated_results.append(record)
    
    return {
        'success': True,
        'table': table_name,
        'results': simulated_results,
        'count': len(simulated_results)
    }

def simulate_insert_query(parameters):
    """Simulate an INSERT database query"""
    table_name = parameters.get('table', 'unknown_table')
    data = parameters.get('data', {})
    
    # Simulate validation
    if not data:
        return {'success': False, 'error': 'No data provided for insert'}
    
    # Simulate successful insertion
    return {
        'success': True,
        'table': table_name,
        'inserted_id': 12345,
        'message': f'Successfully inserted record into {table_name}'
    }

def simulate_update_query(parameters):
    """Simulate an UPDATE database query"""
    table_name = parameters.get('table', 'unknown_table')
    conditions = parameters.get('where', {})
    updates = parameters.get('set', {})
    
    if not conditions:
        return {'success': False, 'error': 'No WHERE conditions specified'}
    
    if not updates:
        return {'success': False, 'error': 'No SET values specified'}
    
    # Simulate successful update
    return {
        'success': True,
        'table': table_name,
        'rows_affected': 3,
        'message': f'Successfully updated {table_name}'
    }

def simulate_delete_query(parameters):
    """Simulate a DELETE database query"""
    table_name = parameters.get('table', 'unknown_table')
    conditions = parameters.get('where', {})
    
    if not conditions:
        return {'success': False, 'error': 'No WHERE conditions specified for DELETE'}
    
    # Simulate successful deletion
    return {
        'success': True,
        'table': table_name,
        'rows_deleted': 2,
        'message': f'Successfully deleted records from {table_name}'
    }

class DataProcessor:
    """A comprehensive data processing class"""
    
    def __init__(self):
        self.processing_history = []
        self.error_log = []
        self.configuration = {
            'max_batch_size': 1000,
            'timeout_seconds': 30,
            'retry_attempts': 3
        }
    
    def process_batch(self, data_batch):
        """Process a batch of data"""
        start_time = "simulated_start_time"
        batch_id = f"batch_{len(self.processing_history) + 1}"
        
        try:
            if len(data_batch) > self.configuration['max_batch_size']:
                raise ValueError(f"Batch size exceeds maximum of {self.configuration['max_batch_size']}")
            
            processed_items = []
            for item in data_batch:
                processed_item = self._process_single_item(item)
                processed_items.append(processed_item)
            
            # Log successful processing
            processing_record = {
                'batch_id': batch_id,
                'start_time': start_time,
                'end_time': "simulated_end_time",
                'items_processed': len(processed_items),
                'success': True
            }
            self.processing_history.append(processing_record)
            
            return {
                'batch_id': batch_id,
                'success': True,
                'processed_items': processed_items,
                'processing_time': "simulated_duration"
            }
            
        except Exception as e:
            error_record = {
                'batch_id': batch_id,
                'error': str(e),
                'timestamp': "simulated_error_time"
            }
            self.error_log.append(error_record)
            
            return {
                'batch_id': batch_id,
                'success': False,
                'error': str(e)
            }
    
    def _process_single_item(self, item):
        """Process a single data item"""
        if item is None:
            return {'status': 'skipped', 'reason': 'null_item'}
        
        if isinstance(item, str):
            return {
                'original': item,
                'processed': item.strip().upper(),
                'type': 'string',
                'status': 'processed'
            }
        elif isinstance(item, (int, float)):
            return {
                'original': item,
                'processed': item * 1.1,
                'type': 'numeric',
                'status': 'processed'
            }
        else:
            return {
                'original': str(item),
                'processed': str(item),
                'type': 'other',
                'status': 'converted'
            }
    
    def get_processing_statistics(self):
        """Get statistics about processing history"""
        total_batches = len(self.processing_history)
        successful_batches = sum(1 for record in self.processing_history if record['success'])
        total_items = sum(record['items_processed'] for record in self.processing_history)
        total_errors = len(self.error_log)
        
        return {
            'total_batches': total_batches,
            'successful_batches': successful_batches,
            'failed_batches': total_batches - successful_batches,
            'total_items_processed': total_items,
            'total_errors': total_errors,
            'success_rate': successful_batches / total_batches if total_batches > 0 else 0
        }

class ConfigurationManager:
    """Manage application configuration"""
    
    def __init__(self):
        self.config_data = {}
        self.config_history = []
        self.validation_rules = {}
    
    def set_configuration(self, key, value, validate=True):
        """Set a configuration value"""
        if validate and key in self.validation_rules:
            validation_result = self._validate_config_value(key, value)
            if not validation_result['valid']:
                return {
                    'success': False,
                    'error': validation_result['error']
                }
        
        old_value = self.config_data.get(key)
        self.config_data[key] = value
        
        # Record the change
        change_record = {
            'key': key,
            'old_value': old_value,
            'new_value': value,
            'timestamp': "simulated_timestamp"
        }
        self.config_history.append(change_record)
        
        return {
            'success': True,
            'key': key,
            'value': value
        }
    
    def get_configuration(self, key, default=None):
        """Get a configuration value"""
        return self.config_data.get(key, default)
    
    def add_validation_rule(self, key, rule_type, rule_params):
        """Add a validation rule for a configuration key"""
        self.validation_rules[key] = {
            'type': rule_type,
            'params': rule_params
        }
    
    def _validate_config_value(self, key, value):
        """Validate a configuration value against its rules"""
        rule = self.validation_rules.get(key)
        if not rule:
            return {'valid': True}
        
        rule_type = rule['type']
        rule_params = rule['params']
        
        if rule_type == 'type_check':
            expected_type = rule_params['type']
            if not isinstance(value, expected_type):
                return {
                    'valid': False,
                    'error': f'Expected {expected_type.__name__}, got {type(value).__name__}'
                }
        
        elif rule_type == 'range_check':
            min_val = rule_params.get('min')
            max_val = rule_params.get('max')
            if min_val is not None and value < min_val:
                return {'valid': False, 'error': f'Value below minimum {min_val}'}
            if max_val is not None and value > max_val:
                return {'valid': False, 'error': f'Value above maximum {max_val}'}
        
        elif rule_type == 'choices':
            valid_choices = rule_params['choices']
            if value not in valid_choices:
                return {'valid': False, 'error': f'Value must be one of {valid_choices}'}
        
        return {'valid': True}

def utility_function_comprehensive_test():
    """Comprehensive test function to validate various utilities"""
    test_results = {}
    
    # Test basic arithmetic
    test_results['arithmetic'] = {
        'add_result': add_numbers(10, 5),
        'subtract_result': subtract_numbers(10, 5),
        'area_result': calculate_area(5, 4)
    }
    
    # Test calculator class
    calc = Calculator()
    test_results['calculator'] = {
        'add_result': calc.add(8, 3),
        'multiply_result': calc.multiply(4, 6),
        'history_length': len(calc.history)
    }
    
    # Test data processing
    sample_data = ["hello", "world", 123, 45.67, None, "test"]
    test_results['data_processing'] = {
        'method_1': data_processor_function_1(sample_data),
        'method_2': data_processor_function_2(sample_data)
    }
    
    # Test string manipulation
    test_string = "  Hello World  Test String  "
    test_results['string_manipulation'] = {
        'method_1': string_manipulation_function_1(test_string),
        'method_2': string_manipulation_function_2(test_string)
    }
    
    # Test mathematical operations
    number_list = [1, 5, 3, 9, 2, 7, 4, 8, 6]
    test_results['mathematical_operations'] = mathematical_operations_function_1(number_list)
    
    # Test file operations
    test_results['file_operations'] = file_operations_function_1("test_file.txt")
    
    # Test database simulation
    test_results['database_operations'] = {
        'select': database_simulation_function_1("SELECT", {"table": "users"}),
        'insert': database_simulation_function_1("INSERT", {"table": "users", "data": {"name": "John", "age": 30}})
    }
    
    # Test data processor class
    processor = DataProcessor()
    batch_data = ["item1", "item2", 42, 3.14, "item5"]
    test_results['batch_processing'] = {
        'batch_result': processor.process_batch(batch_data),
        'statistics': processor.get_processing_statistics()
    }
    
    # Test configuration manager
    config_mgr = ConfigurationManager()
    config_mgr.add_validation_rule('max_connections', 'range_check', {'min': 1, 'max': 100})
    test_results['configuration'] = {
        'set_result': config_mgr.set_configuration('max_connections', 50),
        'get_result': config_mgr.get_configuration('max_connections')
    }
    
    return test_results

if __name__ == "__main__":
    print("Running comprehensive test suite...")
    
    # Run individual function tests
    print("Testing basic functions...")
    calc = Calculator()
    print(f"Addition: {calc.add(5, 3)}")
    print(f"Multiplication: {calc.multiply(4, 7)}")
    print(f"Calculator history: {calc.history}")
    
    # Run comprehensive test
    print("\nRunning comprehensive tests...")
    comprehensive_results = utility_function_comprehensive_test()
    print("Comprehensive test completed successfully!")
    
    # Test data validation
    test_data = [1, 2, 3, 4, 5]
    validation_result = data_validator_function_1(test_data)
    print(f"Data validation result: {validation_result}")
    
    print("All tests completed!")
