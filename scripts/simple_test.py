def add_numbers(a, b):
    """Add two numbers and return the result"""
    return a + b
# test main 

def subtract_numbers(a, b):
    """Subtract second number from first"""
    return a - b

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

if __name__ == "__main__":
    calc = Calculator()
    print(calc.add(5, 3))
    print(calc.multiply(4, 7))
    print("Calculator history:", calc.history)
