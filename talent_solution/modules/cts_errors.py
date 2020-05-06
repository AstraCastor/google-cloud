#Custom Error Classes
class UnparseableJobError(Exception):
    pass

class UnknownCompanyError(Exception):
    pass

class UnknownTenantError(Exception):
    pass

class CTSSchemaError(Exception):
    pass