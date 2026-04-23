const ENTITY_MAP: Record<string, string> = {
  amp: "&",
  apos: "'",
  bull: "*",
  gt: ">",
  hellip: "...",
  laquo: "<<",
  ldquo: '"',
  lsquo: "'",
  lt: "<",
  nbsp: " ",
  ndash: "-",
  mdash: "-",
  quot: '"',
  raquo: ">>",
  rdquo: '"',
  rsquo: "'",
};

function decodeHtmlEntities(value: string): string {
  return value.replace(/&(#x?[0-9a-f]+|[a-z][a-z0-9]+);/gi, (match, entity) => {
    const key = String(entity).toLowerCase();
    if (key.startsWith("#x")) {
      const codePoint = Number.parseInt(key.slice(2), 16);
      return Number.isFinite(codePoint) && codePoint >= 0 && codePoint <= 0x10ffff ? String.fromCodePoint(codePoint) : match;
    }
    if (key.startsWith("#")) {
      const codePoint = Number.parseInt(key.slice(1), 10);
      return Number.isFinite(codePoint) && codePoint >= 0 && codePoint <= 0x10ffff ? String.fromCodePoint(codePoint) : match;
    }
    return ENTITY_MAP[key] ?? match;
  });
}

export function cleanDisplayText(value: unknown): string {
  const raw = typeof value === "string" ? value : value == null ? "" : String(value);
  const decoded = decodeHtmlEntities(raw)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<\/(p|div|li|ul|ol|h[1-6]|tr|section|article)>/gi, " ")
    .replace(/<[^>]+>/g, " ");
  return decodeHtmlEntities(decoded).replace(/\s+/g, " ").trim();
}

function humanizeField(value: unknown): string {
  const text = cleanDisplayText(value).replace(/[_-]+/g, " ").trim();
  if (!text || text.toLowerCase() === "body") {
    return "";
  }
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function formatApiDetailItem(item: unknown): string | null {
  if (typeof item === "string") {
    return cleanDisplayText(item) || null;
  }
  if (!item || typeof item !== "object") {
    return null;
  }

  const record = item as Record<string, unknown>;
  const message = cleanDisplayText(record.msg ?? record.detail ?? record.message);
  if (!message) {
    return null;
  }

  const loc = Array.isArray(record.loc) ? record.loc.map(humanizeField).filter(Boolean) : [];
  const field = loc.length ? loc[loc.length - 1] : "";
  return field ? `${field}: ${message}` : message;
}

export function formatApiErrorDetail(detail: unknown): string | null {
  if (Array.isArray(detail)) {
    const messages = detail.map(formatApiDetailItem).filter(Boolean) as string[];
    return messages.length ? messages.join(" ") : null;
  }
  return formatApiDetailItem(detail);
}
