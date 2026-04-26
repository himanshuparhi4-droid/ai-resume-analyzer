export const DEFAULT_SITE_URL = "https://ai-resume-analyzer-xi-sepia.vercel.app";

export const SITE_URL = ((import.meta.env.VITE_SITE_URL as string | undefined) || DEFAULT_SITE_URL).replace(/\/$/, "");

export type SeoConfig = {
  title: string;
  description: string;
  path: string;
  robots?: string;
};

export const DEFAULT_SEO: SeoConfig = {
  title: "AI Resume Analyzer | ATS Resume Feedback and Career Insights",
  description:
    "Upload your resume and get AI-powered feedback on ATS compatibility, keywords, formatting, skills, readability, and role fit.",
  path: "/",
  robots: "index, follow",
};

export const ROUTE_SEO: Record<string, SeoConfig> = {
  "/": DEFAULT_SEO,
  "/upload": {
    title: "Upload Resume for AI Analysis | Resume Intelligence Studio",
    description:
      "Upload your resume securely and receive feedback on ATS compatibility, formatting, keywords, skills, readability, and recruiter appeal.",
    path: "/upload",
    robots: "index, follow",
  },
  "/pricing": {
    title: "Resume Analysis Pricing in India | Resume Intelligence Studio",
    description:
      "Compare free and paid resume analysis plans in Indian rupees, including ATS checks, detailed AI feedback, and premium optimization.",
    path: "/pricing",
    robots: "index, follow",
  },
  "/about": {
    title: "About Resume Intelligence Studio | AI Resume Analysis",
    description:
      "Learn how Resume Intelligence Studio analyzes resumes using ATS best practices, recruiter readability, skill evidence, and market signals.",
    path: "/about",
    robots: "index, follow",
  },
  "/dashboard": {
    title: "Resume Analysis Dashboard | Resume Intelligence Studio",
    description: "View your private resume score dashboard after uploading and analyzing a resume.",
    path: "/dashboard",
    robots: "noindex, follow",
  },
  "/feedback": {
    title: "Detailed Resume Feedback | Resume Intelligence Studio",
    description: "Review private section-by-section resume feedback for summary, experience, skills, education, formatting, and ATS readiness.",
    path: "/feedback",
    robots: "noindex, follow",
  },
  "/suggestions": {
    title: "Resume Improvement Suggestions | Resume Intelligence Studio",
    description: "View private resume improvement suggestions, missing keywords, rewrite examples, and priority fixes.",
    path: "/suggestions",
    robots: "noindex, follow",
  },
  "/skills": {
    title: "Resume Skill Gap Analysis | Resume Intelligence Studio",
    description: "Review private skill evidence, market gaps, matched skills, and weak-proof signals from your resume analysis.",
    path: "/skills",
    robots: "noindex, follow",
  },
  "/jobs": {
    title: "Resume Matched Job Listings | Resume Intelligence Studio",
    description: "View private job matches and apply links generated from your resume analysis.",
    path: "/jobs",
    robots: "noindex, follow",
  },
  "/login": {
    title: "Sign In to Save Resume Reports | Resume Intelligence Studio",
    description: "Sign in or create an optional account to save resume reports, compare versions, and track improvements over time.",
    path: "/login",
    robots: "noindex, follow",
  },
  "/reports": {
    title: "Saved Resume Reports | Resume Intelligence Studio",
    description: "View private saved resume analyses, report history, and version comparisons.",
    path: "/reports",
    robots: "noindex, follow",
  },
};

export function getSeoForPath(pathname: string): SeoConfig {
  return ROUTE_SEO[pathname] ?? {
    title: "Page Not Found | Resume Intelligence Studio",
    description: "The page you requested could not be found. Return to Resume Intelligence Studio to analyze your resume.",
    path: pathname,
    robots: "noindex, follow",
  };
}

export function getCanonicalUrl(path: string): string {
  if (path === "/") {
    return `${SITE_URL}/`;
  }
  return `${SITE_URL}${path}`;
}

export function getWebApplicationSchema() {
  return {
    "@context": "https://schema.org",
    "@type": "WebApplication",
    name: "Resume Intelligence Studio",
    url: `${SITE_URL}/`,
    applicationCategory: "BusinessApplication",
    operatingSystem: "Web",
    description: DEFAULT_SEO.description,
    offers: [
      {
        "@type": "Offer",
        name: "Free Resume Scan",
        price: "0",
        priceCurrency: "INR",
      },
      {
        "@type": "Offer",
        name: "Detailed AI Feedback",
        price: "199",
        priceCurrency: "INR",
      },
      {
        "@type": "Offer",
        name: "Premium Resume Optimization",
        price: "499",
        priceCurrency: "INR",
      },
    ],
  };
}
