import type {
  AdminActionResponse,
  AdminListingActionInput,
  AdminRadarListingResponse,
  ApiErrorResponse,
  AuditLogResponse,
  BillingCheckoutInput,
  BillingWebhookResponse,
  CheckoutSessionResponse,
  CreateMessageInput,
  HealthResponse,
  InvoiceResponse,
  MarketplaceListingResponse,
  MessageResponse,
  PublicListingItem,
  PublishListingInput,
  ScanDecisionResponse,
  SubscriptionResponse,
} from './generated/types';

export type TinssitUploadFile =
  | Blob
  | File
  | {
      uri: string;
      name: string;
      type: string;
    };

export type TinssitLanguage = 'ar' | 'fr';

export type TinssitClientConfig = {
  baseUrl: string;
  apiKey: string;
  language?: TinssitLanguage;
  fetchImpl?: typeof fetch;
  getAccessToken?: () => string | null | undefined | Promise<string | null | undefined>;
};

export type ScanExteriorRequest = {
  clientUuid: string;
  userId: string;
  exteriorFiles: TinssitUploadFile[];
  interiorFile?: TinssitUploadFile | null;
  weight?: number | null;
  magnetic?: boolean | null;
  latitude?: number | null;
  longitude?: number | null;
  language?: TinssitLanguage;
};

export class TinssitApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly payload?: ApiErrorResponse;

  constructor(status: number, payload?: ApiErrorResponse) {
    super(payload?.error.message ?? `Tinssit API error ${status}`);
    this.name = 'TinssitApiError';
    this.status = status;
    this.code = payload?.error.code ?? 'HTTP_ERROR';
    this.payload = payload;
  }
}

export function createTinssitClient(config: TinssitClientConfig) {
  const baseUrl = config.baseUrl.replace(/\/+$/, '');
  const fetcher = config.fetchImpl ?? fetch;

  async function requestJson<T>(
    path: string,
    init: RequestInit = {},
    options: { auth?: boolean; language?: TinssitLanguage } = {},
  ): Promise<T> {
    const headers = new Headers(init.headers);
    const language = options.language ?? config.language;
    if (language) {
      headers.set('Accept-Language', language);
    }
    if (options.auth !== false) {
      headers.set('X-API-Key', config.apiKey);
      const accessToken = await config.getAccessToken?.();
      if (accessToken) {
        headers.set('Authorization', `Bearer ${accessToken}`);
      }
    }

    const response = await fetcher(`${baseUrl}${path}`, {
      ...init,
      headers,
    });

    const payload = await readJson(response);
    if (!response.ok) {
      throw new TinssitApiError(response.status, payload as ApiErrorResponse | undefined);
    }

    return payload as T;
  }

  return {
    health: () => requestJson<HealthResponse>('/health', {}, { auth: false }),

    scanExterior: (input: ScanExteriorRequest) =>
      requestJson<ScanDecisionResponse>(
        '/api/v1/scan/exterior',
        {
          method: 'POST',
          body: buildScanExteriorFormData(input),
        },
        { language: input.language },
      ),

    scanInteriorUpdate: (
      scanId: string,
      fileInterior: TinssitUploadFile,
      options: { language?: TinssitLanguage } = {},
    ) =>
      requestJson<ScanDecisionResponse>(
        `/api/v1/scan/${encodeURIComponent(scanId)}/interior`,
        {
          method: 'PATCH',
          body: buildSingleFileFormData('file_interior', fileInterior),
        },
        { language: options.language },
      ),

    publishScanToMarketplace: (scanId: string, payload: PublishListingInput = {}) =>
      requestJson<MarketplaceListingResponse>(
        `/api/v1/marketplace/publish/${encodeURIComponent(scanId)}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        },
      ),

    getMarketplaceListings: () =>
      requestJson<PublicListingItem[]>('/api/v1/marketplace/listings'),

    listAdminRadar: () => requestJson<AdminRadarListingResponse[]>('/api/v1/admin/radar'),

    reserveAdminRadarListing: (listingId: string, payload: AdminListingActionInput = {}) =>
      requestJson<AdminActionResponse>(
        `/api/v1/admin/radar/${encodeURIComponent(listingId)}/reserve`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        },
      ),

    releaseAdminRadarListing: (listingId: string, payload: AdminListingActionInput = {}) =>
      requestJson<AdminActionResponse>(
        `/api/v1/admin/radar/${encodeURIComponent(listingId)}/release`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        },
      ),

    rejectAdminRadarListing: (listingId: string, payload: AdminListingActionInput = {}) =>
      requestJson<AdminActionResponse>(
        `/api/v1/admin/radar/${encodeURIComponent(listingId)}/reject`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        },
      ),

    listAdminAuditLogs: (limit = 50) =>
      requestJson<AuditLogResponse[]>(`/api/v1/admin/audit?limit=${limit}`),

    createBillingCheckout: (payload: BillingCheckoutInput) =>
      requestJson<CheckoutSessionResponse>('/api/v1/billing/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }),

    getBillingSubscription: () =>
      requestJson<SubscriptionResponse>('/api/v1/billing/subscription'),

    cancelBillingSubscription: () =>
      requestJson<SubscriptionResponse>('/api/v1/billing/cancel', {
        method: 'POST',
      }),

    listBillingInvoices: () => requestJson<InvoiceResponse[]>('/api/v1/billing/invoices'),

    sendBillingWebhook: (provider: string, payload: Record<string, unknown>) =>
      requestJson<BillingWebhookResponse>(
        `/api/v1/billing/webhooks/${encodeURIComponent(provider)}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        },
      ),

    sendChatMessage: (payload: CreateMessageInput) =>
      requestJson<MessageResponse>('/api/v1/marketplace/chat/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }),

    getChatHistory: (conversationId: string) =>
      requestJson<MessageResponse[]>(
        `/api/v1/marketplace/chat/history/${encodeURIComponent(conversationId)}`,
      ),
  };
}

function buildScanExteriorFormData(input: ScanExteriorRequest) {
  const formData = new FormData();
  formData.append('client_uuid', input.clientUuid);
  formData.append('user_id', input.userId);
  for (const file of input.exteriorFiles) {
    appendFile(formData, 'files_exterior', file);
  }
  if (input.interiorFile) {
    appendFile(formData, 'file_interior', input.interiorFile);
  }
  appendOptional(formData, 'weight', input.weight);
  appendOptional(formData, 'magnetic', input.magnetic);
  appendOptional(formData, 'latitude', input.latitude);
  appendOptional(formData, 'longitude', input.longitude);
  return formData;
}

function buildSingleFileFormData(field: string, file: TinssitUploadFile) {
  const formData = new FormData();
  appendFile(formData, field, file);
  return formData;
}

function appendOptional(formData: FormData, field: string, value?: string | number | boolean | null) {
  if (value !== undefined && value !== null) {
    formData.append(field, String(value));
  }
}

function appendFile(formData: FormData, field: string, file: TinssitUploadFile) {
  formData.append(field, file as Blob);
}

async function readJson(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) {
    return undefined;
  }

  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}
