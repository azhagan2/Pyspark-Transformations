import sys
import types

class MockGlueModule(types.ModuleType):
    def __getattr__(self, item):
        return lambda *args, **kwargs: None

sys.modules['awsglue'] = MockGlueModule('awsglue')
sys.modules['awsglue.context'] = MockGlueModule('awsglue.context')
sys.modules['awsglue.job'] = MockGlueModule('awsglue.job')
sys.modules['awsglue.utils'] = MockGlueModule('awsglue.utils')
