

class FunctionsList(object):
    __functions_dict = {}

    @staticmethod
    def total_functions():
        return len(FunctionsList.__functions_dict)

    @staticmethod
    def add_function(name, value):
        FunctionsList.__functions_dict[name] = value

    @staticmethod
    def remove_function(name):
        if FunctionsList.is_function_exist(name):
            del FunctionsList.__functions_dict[name]

    @staticmethod
    def get_function(name):
        return FunctionsList.__functions_dict[name]

    @staticmethod
    def is_function_exist(name):
        if FunctionsList.__functions_dict[name]:
            return True
        return False


