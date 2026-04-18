import type { Config } from "tailwindcss";

export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#06131c",
        mist: "#f3f7f4",
        sea: "#5ec2b7",
        ember: "#ff8a5b",
        gold: "#f4c95d",
        slate: "#7c8b95"
      },
      boxShadow: {
        soft: "0 20px 70px rgba(5, 19, 28, 0.12)"
      },
      fontFamily: {
        display: ["'Space Grotesk'", "sans-serif"],
        body: ["'Manrope'", "sans-serif"],
        mono: ["'IBM Plex Mono'", "monospace"]
      }
    }
  },
  plugins: []
} satisfies Config;
