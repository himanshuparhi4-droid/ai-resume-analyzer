import { useEffect } from "react";
import { useLocation } from "react-router-dom";
import { getCanonicalUrl, getSeoForPath, getWebApplicationSchema, SITE_URL } from "../lib/seo";

const OG_IMAGE_URL = `${SITE_URL}/og-image.svg`;

function upsertMeta(selector: string, attribute: "name" | "property", key: string, content: string) {
  let element = document.head.querySelector<HTMLMetaElement>(selector);
  if (!element) {
    element = document.createElement("meta");
    element.setAttribute(attribute, key);
    document.head.appendChild(element);
  }
  element.setAttribute("content", content);
}

function upsertCanonical(href: string) {
  let element = document.head.querySelector<HTMLLinkElement>('link[rel="canonical"]');
  if (!element) {
    element = document.createElement("link");
    element.setAttribute("rel", "canonical");
    document.head.appendChild(element);
  }
  element.setAttribute("href", href);
}

function upsertJsonLd(id: string, schema: unknown) {
  let element = document.getElementById(id) as HTMLScriptElement | null;
  if (!element) {
    element = document.createElement("script");
    element.type = "application/ld+json";
    element.id = id;
    document.head.appendChild(element);
  }
  element.textContent = JSON.stringify(schema);
}

export function Seo() {
  const location = useLocation();

  useEffect(() => {
    const seo = getSeoForPath(location.pathname);
    const canonical = getCanonicalUrl(seo.path);

    document.title = seo.title;
    upsertMeta('meta[name="description"]', "name", "description", seo.description);
    upsertMeta('meta[name="robots"]', "name", "robots", seo.robots ?? "index, follow");
    upsertMeta('meta[name="googlebot"]', "name", "googlebot", seo.robots ?? "index, follow");
    upsertMeta('meta[property="og:title"]', "property", "og:title", seo.title);
    upsertMeta('meta[property="og:description"]', "property", "og:description", seo.description);
    upsertMeta('meta[property="og:url"]', "property", "og:url", canonical);
    upsertMeta('meta[property="og:image"]', "property", "og:image", OG_IMAGE_URL);
    upsertMeta('meta[property="og:image:alt"]', "property", "og:image:alt", "Resume Intelligence Studio resume analysis dashboard preview");
    upsertMeta('meta[name="twitter:title"]', "name", "twitter:title", seo.title);
    upsertMeta('meta[name="twitter:description"]', "name", "twitter:description", seo.description);
    upsertMeta('meta[name="twitter:image"]', "name", "twitter:image", OG_IMAGE_URL);
    upsertCanonical(canonical);
    upsertJsonLd("app-structured-data", getWebApplicationSchema());
  }, [location.pathname]);

  return null;
}
