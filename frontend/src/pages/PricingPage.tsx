import { Link } from "react-router-dom";

const PLANS = [
  {
    name: "Free Scan",
    price: "Rs. 0",
    description: "A useful first pass for checking resume score, ATS readability, and the main improvement areas.",
    features: ["Resume score", "ATS and formatting signals", "Core strengths and issues", "Basic suggestions"],
  },
  {
    name: "Detailed Feedback",
    price: "Rs. 199",
    description: "A deeper report with section-by-section feedback, missing keywords, and practical improvement priorities.",
    features: ["Detailed feedback", "Keyword gap review", "Skill evidence review", "Written report export"],
  },
  {
    name: "Premium Rewrite",
    price: "Rs. 499",
    description: "More focused rewrite guidance for users who want help turning weak bullets into stronger resume proof.",
    features: ["Rewrite examples", "Version comparison", "Saved reports", "Improvement tracking"],
  },
];

export function PricingPage() {
  return (
    <div className="grid gap-6">
      <section className="glass-panel rounded-[2rem] p-6 sm:p-8">
        <p className="eyebrow">Pricing</p>
        <h1 className="mt-3 font-display text-4xl font-extrabold leading-tight tracking-[-0.055em] text-ink md:text-5xl dark:text-slate-50">
          Simple plans for resume improvement.
        </h1>
        <p className="mt-4 max-w-3xl text-base font-semibold leading-8 text-slate-700 dark:text-slate-300">
          Start with a free scan. Upgrade only if you need deeper feedback, saved reports, or rewrite support.
        </p>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        {PLANS.map((plan, index) => (
          <article key={plan.name} className={`signal-panel rounded-[2rem] p-6 ${index === 1 ? "ring-2 ring-cyan-300/25" : ""}`}>
            <p className="eyebrow">{plan.name}</p>
            <h2 className="mt-4 font-display text-4xl font-extrabold tracking-[-0.055em] text-ink dark:text-slate-50">{plan.price}</h2>
            <p className="mt-3 text-sm font-semibold leading-7 text-slate-700 dark:text-slate-300">{plan.description}</p>
            <div className="mt-5 grid gap-3">
              {plan.features.map((feature) => (
                <p key={feature} className="rounded-[1rem] border border-slate-900/10 bg-white/50 px-4 py-3 text-sm font-bold text-slate-700 dark:border-white/10 dark:bg-white/[0.04] dark:text-slate-300">
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
