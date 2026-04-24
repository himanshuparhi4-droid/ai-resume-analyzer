import type { Config } from "tailwindcss";
import colors from "tailwindcss/colors";

export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#07111f",
        mist: "#eef3f8",
        sea: "#67e8f9",
        ember: "#60a5fa",
        gold: "#34d399",
        slate: {
          ...colors.slate,
          DEFAULT: "#7c8b95",
        }
      },
      boxShadow: {
        soft: "0 20px 70px rgba(5, 19, 28, 0.12)"
      },
      fontFamily: {
        display: ["'Bricolage Grotesque'", "sans-serif"],
        body: ["'Manrope'", "sans-serif"],
        mono: ["'IBM Plex Mono'", "monospace"]
      }
    }
  },
  plugins: []
} satisfies Config;
