
Constraints on design and code.

# Error Handling

Errors should not be caught in code unless one of the exceptions below applies. Retry logic on errors should not be added 
unless planning process specifically identifies the need to do that. The idea is for errors to propogate up and retain full
traceback of the cause.

During the development process, prior to any release completion, errors should not even be caught in the top level of
code, ensuring that issues are exposed with maximum diagnostic information to developer during this process. 

## Exceptions

1. If errors are expected as a normal part of the flow of operations, they may be handled. Examples
   * asyncio.CancelledError 
   * socket errors that might occur because the socket has been closed
   * ValueError when trying to parse text where the format is not fully certain
2. When the code in question has been designated as top layer of the application, and the stage of development has
   been declared to be pre-release hardening. 


# Unrequested features

Anytime the feature or code design process identifies the potential that a feature might be desirable but has not
been specifically requested, an explicit request for permission to add the feature must be made and approved before
the feature is added. The guiding principle is that the code's features should all be directly traceable to an 
elemement of the specification, and reducing code complexity is a first class goal.


