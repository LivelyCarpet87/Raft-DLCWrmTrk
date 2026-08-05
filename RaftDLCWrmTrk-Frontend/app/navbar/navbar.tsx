import {Link, useLocation} from "react-router"

interface HeaderLinkProps {
    to: string,
    displayName: string,
    active: boolean,
}

function HeaderLink({to, displayName, active}: HeaderLinkProps) {
    return <Link to={to} className={`text-lg font-bold ${active ? "text-slate-700" : "text-slate-500"}`}>{displayName}</Link>
}

export function NavBar() {
    const {pathname} = useLocation()
    return (
        <header className="flex flex-row items-center justify-center-safe gap-9 p-6 bg-slate-100 border-b-4 border-slate-200 w-full h-fit sticky top-0 z-10">
            <HeaderLink to="/" displayName="Home" active={pathname === "/"} />
            <HeaderLink to="/experiment-data" displayName="Experiment Data" active={pathname.startsWith("experiment-data")} />
            <HeaderLink to="/" displayName="Experiment Management" active={pathname.startsWith("experiment-management")} />
            <HeaderLink to="/" displayName="System Metrics" active={pathname.startsWith("system-metrics")} />
            <HeaderLink to="/" displayName="Logout" active={false} />
        </header>
    );
}