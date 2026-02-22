# FileMaker MCP Server — Architecture

## System Overview

```
┌─────────────────────────────────────────────────────┐
│  AI Clients                                          │
│  ├─ Claude Desktop  (interactive sessions)           │
│  ├─ Claude Code     (development/agentic coding)     │
│  └─ Future: custom chat interfaces                   │
└──────────────────┬──────────────────────────────────┘
                   │ MCP Protocol (stdio or SSE)
                   │
┌──────────────────▼──────────────────────────────────┐
│  FileMaker MCP Server (Python / FastMCP)                 │
│                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   Tools     │  │  Resources  │  │   Config    │ │
│  │ (callable)  │  │ (context)   │  │ (settings)  │ │
│  ├─────────────┤  ├─────────────┤  ├─────────────┤ │
│  │ query       │  │ DDL ref     │  │ .env        │ │
│  │ get_record  │  │ biz rules   │  │ credentials │ │
│  │ count       │  │ tenant cfg  │  │ endpoints   │ │
│  │ schema      │  │             │  │             │ │
│  │ list_tables │  │             │  │             │ │
│  └──────┬──────┘  └─────────────┘  └─────────────┘ │
│         │                                            │
│  ┌──────▼──────┐                                    │
│  │  Auth Layer │                                    │
│  │ OData Basic │                                    │
│  │ DataAPI Tok │                                    │
│  └──────┬──────┘                                    │
└─────────┼───────────────────────────────────────────┘
          │ HTTPS
┌─────────▼───────────────────────────────────────────┐
│  FileMaker Server 22  (your-server.example.com)       │
│  ├─ OData v4 API    (/fmi/odata/v4/)                │
│  │   Basic auth, read queries, $metadata             │
│  ├─ Data API        (/fmi/data/vLatest/)             │
│  │   Session tokens, CRUD, script execution          │
│  └─ OData Webhooks  (Phase 5)                        │
│      Push notifications on data changes              │
└─────────────────────────────────────────────────────┘
```

## Design Principles

1. **FileMaker is the system of record.** The MCP server is a read window (Phase 1)
   that will expand to controlled write access in later phases.

2. **No Claris dependency.** This server talks directly to FM Server APIs.
   No Claris Studio, no Claris MCP proxy, no cloud middleman.

3. **Security by default.** Read-only FM account, exposed table whitelist,
   no credential leakage. Write access requires explicit Phase 2 implementation.

4. **Self-describing tools.** Every MCP tool has comprehensive docstrings that
   serve as the AI's instruction manual. The AI learns what it can do from
   the tool metadata — no separate prompt engineering needed.

5. **Async throughout.** httpx async client + FastMCP async handlers.
   No blocking calls, clean connection management.

## Phase Roadmap

### Phase 1: Foundation (Current)
- OData read-only queries
- Schema discovery
- Basic auth
- stdio transport (local only)
- Claude Desktop + Code integration

### Phase 2: CRUD + Scripts
- Data API session management
- Create/update/delete records
- FM script execution with parameters
- Transaction-safe write patterns

### Phase 3: Context Layer
- DDL as MCP Resources (auto-loaded context)
- Business rules documentation
- Tenant configuration
- Query templates / common patterns

### Phase 4: External Services
- GHL tools (contact sync, conversation AI)
- Pipedream workflow triggers
- Google APIs (Maps, Ads)
- Multi-service orchestration

### Phase 5: Remote / Multi-tenant
- SSE transport for remote access
- Tenant routing (location_id based)
- Deploy to Fly.io or similar
- OData webhook integration for real-time sync
- CI/CD pipeline for automated deployment

## Security Model

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| FM Account | `mcp_agent` with scoped privilege set | Table-level access control |
| Transport | stdio (local only, Phase 1) | No network exposure |
| Credentials | .env file, never in code | Git-ignored |
| Tool Whitelist | EXPOSED_TABLES dict | Only listed tables queryable |
| Result Limits | Max 100 records per query | Prevents context overflow |
| Audit | FM Server access logs | All queries logged server-side |
