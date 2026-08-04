import type { Route } from "./+types/home";
import ExperimentData from "~/experiment-data/experiment-data";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "New React Router App" },
    { name: "description", content: "Welcome to React Router!" },
  ];
}

export default function Home() {
  return <ExperimentData />;
}