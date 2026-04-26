import { Link } from "react-router-dom";

const PLANS = [
  {
    name: "Free Resume Scan",
    price: "Rs. 0",
    note: "Starter scan",
    description: "Start with a practical resume scan covering ATS readability, role fit, and core improvement areas.",
    features: ["Anonymous resume analysis", "Overall score", "Core ATS and keyword feedback", "Actionable next steps"],
  },
  {
    name: "Detailed AI Feedback",
    price: "Rs. 199",
    note: "Per detailed report",
    description: "Unlock deeper section feedback, skill evidence review, and detailed recommendations for stronger applications.",
    features: ["Detailed feedback sections", "Missing keyword analysis", "Weak proof detection", "Market-informed suggestions"],
  },
  {
    name: "Premium Resume Optimization",
    price: "Rs. 499",
    note: "Per resume version",
    description: "For users who want richer rewrite suggestions, version tracking, and ongoing resume improvement support.",
    features: ["Rewrite guidance", "Saved reports", "Version comparison", "Progress tracking"],
  },
];

export function PricingPage() {
  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <p className="eyebrow">Pricing</p>
        <h1 className="mt-3 font-display text-5xl font-extrabold leading-none tracking-[-0.06em] text-ink dark:text-slate-50">
          Start with a useful free resume scan.
        </h1>
        <p className="mt-4 max-w-3xl text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">
          Start with a free resume scan or unlock advanced feedback, detailed recommendations, and AI-powered rewrite suggestions.
        </p>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        {PLANS.map((plan, index) => (
          <article key={plan.name} className={`signal-panel rounded-[2rem] p-6 ${index === 1 ? "ring-2 ring-cyan-300/30" : ""}`}>
            <p className="eyebrow">{plan.name}</p>
            <h2 className="mt-4 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">{plan.price}</h2>
            <p className="mt-1 text-xs font-black uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">{plan.note}</p>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{plan.description}</p>
            <div className="mt-5 grid gap-3">
              {plan.features.map((feature) => (
                <p key={feature} className="rounded-[1rem] bg-white/45 px-4 py-3 text-sm font-bold text-slate-700 dark:bg-white/[0.05] dark:text-slate-300">
                  {feature}
                </p>
              ))}
            </div>
            <Link className="primary-button mt-6 w-full" to="/upload">
              Analyze My Resume
            </Link>
          </article>
        ))}
      </section>
    </div>
  );
}
