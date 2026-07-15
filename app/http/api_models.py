from __future__ import annotations

from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProblemFieldError(StrictModel):
    location: list[str | int]
    message: str
    code: str


class ProblemDetails(StrictModel):
    type: str
    title: str
    status: int
    detail: str
    instance: str
    code: str
    request_id: str
    errors: list[ProblemFieldError] | None = None


class PageInfo(StrictModel):
    limit: int
    has_more: bool
    next_cursor: str | None = None


DataT = TypeVar("DataT")


class Envelope(StrictModel, Generic[DataT]):
    data: DataT
    page: PageInfo | None = None
    request_id: str


class PrincipalOut(StrictModel):
    id: str
    name: str
    kind: Literal["admin", "service", "public"]
    scopes: list[str]
    domain_grant_mode: Literal["none", "selected", "all"]
    domain_ids: list[int]
    mailbox_patterns: list[str]


class DnsRecommendationOut(StrictModel):
    name: str
    type: str
    value: str
    purpose: str


class DomainOut(StrictModel):
    id: int
    root_domain_ascii: str
    root_domain_unicode: str | None = None
    accept_exact: bool
    accept_subdomains: bool
    public_web_enabled: bool
    public_api_enabled: bool
    is_active: bool
    is_hidden: bool
    local_part_case_sensitive: bool
    plus_addressing_mode: Literal["keep", "strip"]
    max_message_size_bytes: int
    retention_days: int | None = None
    dns_status: Literal["unknown", "ok", "warning", "error"]
    dns_last_checked_at: str | None = None
    dns_details: Any | None = None
    notes: str | None = None
    created_at: str
    updated_at: str
    dns_recommendations: list[DnsRecommendationOut] = Field(default_factory=list)


class DomainCreate(StrictModel):
    root_domain: str = Field(min_length=1, max_length=253)
    accept_exact: bool = True
    accept_subdomains: bool = True
    public_web_enabled: bool = False
    public_api_enabled: bool = False
    plus_addressing_mode: Literal["keep", "strip"] = "keep"
    local_part_case_sensitive: bool = False
    is_active: bool = True
    max_message_size_bytes: int = Field(default=52_428_800, ge=1, le=1_073_741_824)
    retention_days: int | None = Field(default=None, ge=1, le=36_500)


class DomainUpdate(StrictModel):
    root_domain: str | None = Field(default=None, min_length=1, max_length=253)
    accept_exact: bool | None = None
    accept_subdomains: bool | None = None
    public_web_enabled: bool | None = None
    public_api_enabled: bool | None = None
    local_part_case_sensitive: bool | None = None
    is_active: bool | None = None
    is_hidden: bool | None = None
    plus_addressing_mode: Literal["keep", "strip"] | None = None
    max_message_size_bytes: int | None = Field(default=None, ge=1, le=1_073_741_824)
    retention_days: int | None = Field(default=None, ge=1, le=36_500)
    notes: str | None = Field(default=None, max_length=4000)

    @model_validator(mode="after")
    def require_update(self) -> "DomainUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one domain field is required")
        return self


class MailboxOut(StrictModel):
    id: int
    domain_id: int
    root_domain_ascii: str
    local_part_canonical: str
    rcpt_domain_ascii: str
    address_canonical: str
    address_display: str
    first_seen_at: str
    last_seen_at: str
    latest_message_at: str | None = None
    message_count: int
    public_enabled: bool
    is_hidden: bool
    notes: str | None = None


class MailboxUpdate(StrictModel):
    public_enabled: bool | None = None
    is_hidden: bool | None = None
    notes: str | None = Field(default=None, max_length=4000)

    @model_validator(mode="after")
    def require_update(self) -> "MailboxUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one mailbox field is required")
        return self


class MessageSummaryOut(StrictModel):
    id: str
    subject: str | None = None
    from_addr: str | None = None
    recipients: str
    received_at: str
    parse_status: Literal["pending", "parsed", "failed"]
    parse_error: str | None = None
    has_attachments: bool
    attachment_count: int
    delivery_count: int


class DeliveryOut(StrictModel):
    delivery_id: str
    mailbox_id: int
    mailbox: str
    rcpt_to: str
    delivered_at: str
    status: Literal["active", "deleted", "hidden"]
    deleted_at: str | None = None
    expires_at: str | None = None


class AttachmentOut(StrictModel):
    id: str
    filename: str | None = None
    safe_filename: str | None = None
    content_type: str | None = None
    content_disposition: str | None = None
    content_id: str | None = None
    size_bytes: int
    is_inline: bool


class MessageDetailOut(StrictModel):
    id: str
    smtp_session_id: str | None = None
    raw_sha256: str
    raw_size_bytes: int
    envelope_from: str | None = None
    message_id_header: str | None = None
    subject: str | None = None
    from_name: str | None = None
    from_addr: str | None = None
    reply_to: str | None = None
    date_header: str | None = None
    received_at: str
    indexed_at: str | None = None
    parse_status: Literal["pending", "parsed", "failed"]
    parse_error: str | None = None
    has_text: bool
    has_html: bool
    has_attachments: bool
    attachment_count: int
    text_preview: str | None = None
    text_body: str
    text_body_source_bytes: int
    text_body_preview_bytes: int
    text_body_truncated: bool
    html_body: str
    html_body_source_bytes: int
    html_body_preview_bytes: int
    html_body_truncated: bool
    headers: list[Any]
    headers_source_bytes: int
    headers_truncated: bool
    inline_preview_embedded_count: int
    inline_preview_skipped_count: int
    inline_preview_embedded_source_bytes: int
    inline_preview_embedded_encoded_bytes: int
    inline_preview_item_limit_bytes: int
    inline_preview_total_limit_bytes: int
    deliveries: list[DeliveryOut]
    attachments: list[AttachmentOut]


class PublicMessageSummaryOut(StrictModel):
    delivery_id: str
    delivered_at: str
    message_id: str
    subject: str | None = None
    from_addr: str | None = None
    verification_code: str | None = None
    has_attachments: bool
    parse_status: Literal["pending", "parsed", "failed"]


class PublicAttachmentOut(StrictModel):
    id: str
    filename: str | None = None
    safe_filename: str | None = None
    content_type: str | None = None
    size_bytes: int
    is_inline: bool


class PublicMessageDetailOut(StrictModel):
    delivery_id: str
    message_id: str
    mailbox: str
    received_at: str
    subject: str | None = None
    from_addr: str | None = None
    verification_code: str | None = None
    parse_status: Literal["pending", "parsed", "failed"]
    text_body: str
    text_body_source_bytes: int
    text_body_preview_bytes: int
    text_body_truncated: bool
    html_body: str
    html_body_source_bytes: int
    html_body_preview_bytes: int
    html_body_truncated: bool
    headers: list[Any]
    headers_source_bytes: int
    headers_truncated: bool
    attachments: list[PublicAttachmentOut]


class VerificationCodeOut(StrictModel):
    delivery_id: str
    message_id: str
    received_at: str
    subject: str | None = None
    from_addr: str | None = None
    parse_status: Literal["pending", "parsed", "failed"]
    verification_code: str | None = None


class SmtpSessionOut(StrictModel):
    id: str
    remote_ip: str
    remote_port: int | None = None
    local_ip: str | None = None
    local_port: int | None = None
    helo_name: str | None = None
    status: Literal["open", "closed", "error"]
    tls_used: bool
    tls_cipher: str | None = None
    tls_protocol: str | None = None
    connect_at: str
    disconnect_at: str | None = None
    first_command_at: str | None = None
    last_command_at: str | None = None
    message_count: int
    rcpt_accepted_count: int
    rcpt_rejected_count: int
    bytes_received: int
    last_mail_from: str | None = None
    last_rcpt_to_sample: str | None = None
    result_code: int | None = None
    result_message: str | None = None
    close_reason: str | None = None


class SmtpEventOut(StrictModel):
    id: int
    seq: int
    event_type: str
    ts: str
    payload: Any | None = None


class SmtpSessionDetailOut(SmtpSessionOut):
    events: list[SmtpEventOut]
    events_page: PageInfo


class ReparseOut(StrictModel):
    message_id: str
    queued: bool


class ResourceDeleteOut(StrictModel):
    id: str
    deleted: bool
    affected: int = 0


class DashboardCacheOut(StrictModel):
    ttl_seconds: float
    age_seconds: float


class DashboardAlertOut(StrictModel):
    severity: Literal["info", "warning", "danger"]
    code: str
    title: str
    detail: str


class DashboardReadPoolOut(StrictModel):
    ok: bool
    state: str
    workers_running: int
    workers_configured: int
    connections_open: int
    outstanding: int
    waiting: int


class DashboardOperationalOut(StrictModel):
    ok: bool
    started: bool | None = None
    stopping: bool | None = None
    parse_queue_running: bool | None = None
    database_read_pool: DashboardReadPoolOut | None = None
    tasks: dict[str, bool] = Field(default_factory=dict)
    error_type: str | None = None


class DashboardHealthOut(StrictModel):
    status: Literal["ok", "warning", "danger"]
    alerts: list[DashboardAlertOut]
    operational: DashboardOperationalOut


class DashboardHttpOut(StrictModel):
    enabled: bool
    requests_total: int
    requests_in_flight: int
    requests_per_second: float | None = None
    rate_window_seconds: float
    rate_kind: Literal["interval", "process_average"]
    p95_ms: float | None = None
    average_ms: float | None = None


class DashboardMailOut(StrictModel):
    received_last_minute: int
    received_last_five_minutes: int
    received_last_day: int
    deliveries_last_minute: int
    deliveries_last_day: int
    rejected_last_day: int
    parse_failures_last_day: int


class DashboardSmtpOut(StrictModel):
    active_connections: int
    python_active_connections: int
    ingestd_active_connections: int | None = None
    open_sessions: int


class DashboardIngestdOut(StrictModel):
    state: Literal["online", "missing", "stale", "invalid"]
    present: bool
    online: bool
    stale: bool
    instance_id: str | None = None
    pid: int | None = None
    updated_at: str | None = None
    age_seconds: float | None = None
    stale_after_seconds: float
    queue_messages: int | None = None
    queue_bytes: int | None = None
    active_connections: int | None = None
    max_connections: int | None = None
    error_type: str | None = None


class DashboardParseQueueOut(StrictModel):
    running: bool
    queued: int
    active_workers: int
    reserved_messages: int
    reserved_bytes: int
    max_messages: int
    max_bytes: int
    pending_messages: int
    failed_messages: int


class DashboardDatabaseOut(StrictModel):
    ok: bool
    error_type: str | None = None
    database_bytes: int
    wal_bytes: int
    shm_bytes: int
    free_bytes: int


class DashboardDiskOut(StrictModel):
    ok: bool
    error_type: str | None = None
    total_bytes: int
    used_bytes: int
    free_bytes: int
    used_percent: float
    warning_threshold_percent: float


class DashboardBackgroundTaskOut(StrictModel):
    name: str
    running: bool
    in_progress: bool
    last_started_at: str | None = None
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error_type: str | None = None
    consecutive_failures: int
    total_successes: int
    total_failures: int
    status: Literal["ok", "warning", "danger"]


class DashboardCleanupOut(StrictModel):
    status: Literal["unknown", "success", "failure"]
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error_type: str | None = None
    consecutive_failures: int


class DashboardRecentMessageOut(StrictModel):
    id: str
    subject: str | None = None
    from_addr: str | None = None
    received_at: str
    parse_status: Literal["pending", "parsed", "failed"]
    attachment_count: int


class DashboardRecentDomainOut(StrictModel):
    id: int
    root_domain_ascii: str
    is_active: bool
    created_at: str


class DashboardDeliveryBucketOut(StrictModel):
    ts: str
    hour: int
    value: int
    x: float
    y: float


class DashboardDeliveryTickOut(StrictModel):
    x: float
    label: str


class DashboardDeliveryChartOut(StrictModel):
    buckets: list[DashboardDeliveryBucketOut]
    peak: int
    total: int
    line_path: str
    area_path: str
    ticks: list[DashboardDeliveryTickOut]
    view_width: int
    view_height: int
    baseline_y: float
    pad_top: float


class DashboardTotalsOut(StrictModel):
    domains: int
    mailboxes: int
    messages: int
    api_keys: int
    audit_logs: int


class DashboardStatusOut(StrictModel):
    generated_at: str
    cache: DashboardCacheOut
    health: DashboardHealthOut
    http: DashboardHttpOut
    mail: DashboardMailOut
    smtp: DashboardSmtpOut
    ingestd: DashboardIngestdOut
    parse_queue: DashboardParseQueueOut
    database: DashboardDatabaseOut
    disk: DashboardDiskOut
    background_tasks: dict[str, DashboardBackgroundTaskOut]
    cleanup: DashboardCleanupOut
    recent_messages: list[DashboardRecentMessageOut]
    recent_domains: list[DashboardRecentDomainOut]
    delivery_chart: DashboardDeliveryChartOut
    totals: DashboardTotalsOut


ApiKeyKind = Literal["admin", "service", "public"]
ApiKeyStatus = Literal["active", "revoked", "expired", "disabled"]
DomainGrantMode = Literal["none", "selected", "all"]


class ApiKeyOut(StrictModel):
    id: int
    public_id: str
    name: str
    description: str | None = None
    kind: ApiKeyKind
    key_prefix: str
    status: ApiKeyStatus
    domain_grant_mode: DomainGrantMode
    allow_header: bool
    allow_query: bool
    rate_limit_per_min: int
    allowed_ip_cidrs: list[str]
    expires_at: str | None = None
    last_used_at: str | None = None
    last_used_ip: str | None = None
    revoked_at: str | None = None
    created_at: str
    scopes: list[str]
    domain_ids: list[int]
    mailbox_patterns: list[str]


class ApiKeyCreate(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    kind: ApiKeyKind
    scopes: list[str] = Field(min_length=1, max_length=100)
    domain_grant_mode: DomainGrantMode = "none"
    domain_ids: list[int] = Field(default_factory=list, max_length=10000)
    mailbox_patterns: list[str] = Field(default_factory=list, max_length=100)
    rate_limit_per_min: int = Field(default=3600, ge=0, le=10_000_000)
    allowed_ip_cidrs: list[str] = Field(default_factory=list, max_length=100)
    expires_at: str | None = None


class ApiKeyUpdate(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=4000)
    status: ApiKeyStatus | None = None
    scopes: list[str] | None = Field(default=None, min_length=1, max_length=100)
    domain_grant_mode: DomainGrantMode | None = None
    domain_ids: list[int] | None = Field(default=None, max_length=10000)
    mailbox_patterns: list[str] | None = Field(default=None, max_length=100)
    rate_limit_per_min: int | None = Field(default=None, ge=0, le=10_000_000)
    allowed_ip_cidrs: list[str] | None = Field(default=None, max_length=100)
    expires_at: str | None = None

    @model_validator(mode="after")
    def require_update(self) -> "ApiKeyUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one API key field is required")
        return self


class ApiKeySecretOut(StrictModel):
    api_key: ApiKeyOut
    secret: str


class ApiKeyActionOut(StrictModel):
    id: int
    status: str
    revoked_at: str | None = None


class MaintenanceResultOut(StrictModel):
    operation: str
    result: dict[str, int | str]


class AuditEventOut(StrictModel):
    id: int
    actor_type: Literal["admin", "api_key", "system", "anonymous"]
    actor_ref: str | None = None
    action: str
    resource_type: str
    resource_ref: str | None = None
    status: Literal["success", "failure"]
    ip: str | None = None
    user_agent: str | None = None
    details: Any | None = None
    created_at: str


class SettingsOut(StrictModel):
    max_message_size_bytes: int
    max_recipients_per_message: int
    smtp_idle_timeout_seconds: int
    smtp_max_concurrent_connections: int
    smtp_connection_rate_limit_count: int
    smtp_connection_rate_limit_window_seconds: int
    disk_warning_threshold_percent: int
    ingress_mode: Literal["managed_only", "managed_plus_catchall"]
    catch_all_public_web_enabled: bool
    catch_all_public_api_enabled: bool
    catch_all_retention_days: int
    retention_cleanup_interval_seconds: int
    smtp_session_retention_seconds: int
    empty_mailbox_retention_seconds: int
    metric_retention_seconds: int
    audit_retention_days: int
    cleanup_batch_size: int
    file_gc_batch_size: int


class SettingsUpdate(StrictModel):
    max_message_size_bytes: int | None = Field(default=None, ge=1, le=1_073_741_824)
    max_recipients_per_message: int | None = Field(default=None, ge=1, le=10_000)
    smtp_idle_timeout_seconds: int | None = Field(default=None, ge=1, le=86_400)
    smtp_max_concurrent_connections: int | None = Field(default=None, ge=0, le=100_000)
    smtp_connection_rate_limit_count: int | None = Field(default=None, ge=0, le=10_000_000)
    smtp_connection_rate_limit_window_seconds: int | None = Field(default=None, ge=1, le=86_400)
    disk_warning_threshold_percent: int | None = Field(default=None, ge=1, le=100)
    ingress_mode: Literal["managed_only", "managed_plus_catchall"] | None = None
    catch_all_public_web_enabled: bool | None = None
    catch_all_public_api_enabled: bool | None = None
    catch_all_retention_days: int | None = Field(default=None, ge=0, le=36_500)
    retention_cleanup_interval_seconds: int | None = Field(default=None, ge=1, le=86_400)
    smtp_session_retention_seconds: int | None = Field(default=None, ge=1, le=315_360_000)
    empty_mailbox_retention_seconds: int | None = Field(default=None, ge=1, le=315_360_000)
    metric_retention_seconds: int | None = Field(default=None, ge=1, le=315_360_000)
    audit_retention_days: int | None = Field(default=None, ge=1, le=36_500)
    cleanup_batch_size: int | None = Field(default=None, ge=1, le=10_000)
    file_gc_batch_size: int | None = Field(default=None, ge=1, le=10_000)

    @model_validator(mode="after")
    def require_update(self) -> "SettingsUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one setting is required")
        return self


AdminRole = Literal["viewer", "operator", "superadmin"]


class AdminOut(StrictModel):
    id: int
    username: str
    display_name: str | None = None
    role: AdminRole
    is_active: bool
    must_change_password: bool
    created_at: str
    updated_at: str
    last_login_at: str | None = None
    last_login_ip: str | None = None


class AdminCreate(StrictModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=12, max_length=1024)
    display_name: str | None = Field(default=None, max_length=200)
    role: AdminRole = "viewer"
    is_active: bool = True
    must_change_password: bool = True


class AdminUpdate(StrictModel):
    username: str | None = Field(default=None, min_length=1, max_length=128)
    display_name: str | None = Field(default=None, max_length=200)
    role: AdminRole | None = None
    is_active: bool | None = None
    must_change_password: bool | None = None

    @model_validator(mode="after")
    def require_update(self) -> "AdminUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one admin field is required")
        return self


class AdminPasswordReset(StrictModel):
    password: str = Field(min_length=12, max_length=1024)
    must_change_password: bool = True


class SessionRevokeOut(StrictModel):
    admin_id: int
    revoked_sessions: int


class DeleteOut(StrictModel):
    id: int
    deleted: bool


__all__ = [
    "AdminCreate",
    "AdminOut",
    "AdminPasswordReset",
    "AdminUpdate",
    "ApiKeyActionOut",
    "ApiKeyCreate",
    "ApiKeyOut",
    "ApiKeySecretOut",
    "ApiKeyUpdate",
    "AttachmentOut",
    "AuditEventOut",
    "DeleteOut",
    "DeliveryOut",
    "DashboardStatusOut",
    "DomainCreate",
    "DomainOut",
    "DomainUpdate",
    "Envelope",
    "MailboxOut",
    "MailboxUpdate",
    "MaintenanceResultOut",
    "MessageDetailOut",
    "MessageSummaryOut",
    "PageInfo",
    "PrincipalOut",
    "ProblemDetails",
    "ProblemFieldError",
    "PublicAttachmentOut",
    "PublicMessageDetailOut",
    "PublicMessageSummaryOut",
    "ReparseOut",
    "ResourceDeleteOut",
    "SessionRevokeOut",
    "SettingsOut",
    "SettingsUpdate",
    "SmtpEventOut",
    "SmtpSessionDetailOut",
    "SmtpSessionOut",
    "VerificationCodeOut",
]
