
import { Outlet } from "react-router";
import { NavBar } from "~/navbar/navbar";


export default function Layout() {
return (
    <main className="flex flex-col items-center justify-start gap-6 pb-4">
        <NavBar/>
        <Outlet/>
    </main>
    );
}