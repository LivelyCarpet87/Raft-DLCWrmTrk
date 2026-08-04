import { type RouteConfig, index, layout, route } from "@react-router/dev/routes";

export default [
    index("routes/home.tsx"),
    layout("layout/layout.tsx", [
        route("experiment-data", "routes/experiment-data.tsx"),
        route("test", "routes/test.tsx"),
    ]),
] satisfies RouteConfig;
