import useSWR from "swr"
import type { Route } from "./+types/home";
import { fetcher } from "~/apiCaller/apiCaller";
import type { ListTagsResponse } from "~/types/apiResponses";

export function meta({}: Route.MetaArgs) {
  return [
    { title: "New React Router App" },
    { name: "description", content: "Welcome to React Router!" },
  ];
}

export default function Home() {
  const { data:ptag, error, isLoading } = useSWR<ListTagsResponse>(
    "/api/experiment/tags/list?tagType=primary",
    fetcher
  );
  console.log(ptag)
  return <div></div>;
}
