# Akka Style Guide

## Messages

> Note that these guidelines specifically apply to public actor messages (those sent between different actors), but it can be a good idea to follow them for internal, private messages as well.

Messages should be case classes or objects, as applicable.

### Declaration

Messages should be declared in the companion object of the actor that receives them.
The exceptions to this rule are as follows:

 - Responses to a request message should be defined in the companion object of the actor that receives the request,
   such that requests and responses are defined together;
 - If a request message is received by multiple actors, then both the requests and responses should be defined
   together in the actor that sends the requests.

### Naming

Messages should be named in the following format:

 - `<verb>[<noun>]`, e.g. `ExecuteEntityTask` is the "request";
 - `[<noun>]<verbed>`, e.g. `EntityTaskExecuted` is the success "reply";
 - `[<noun>]Not<verbed>`, e.g. `EntityTaskNotExecuted` is the failure "reply".

Notes:

 - `<verbed>` is the past tense of `<verb>`;
 - `<noun>` is optional, e.g. `Initialize` is a perfectly fine message name, and would correspond to `Initialized` and `NotInitialized` responses;
 - Not every message must have replies;
 - For "query" type messages, you can use "Fetch" as the verb (or something else).

