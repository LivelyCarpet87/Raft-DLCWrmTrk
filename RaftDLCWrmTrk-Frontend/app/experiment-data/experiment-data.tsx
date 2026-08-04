import CreateBatch from "./create-batch";
import GetBatches from "./get-batches";

export default function ExperimentData() {
    return (
        <div className="flex flex-row gap-6 w-full justify-center justify-items-center-safe items-start place-content-around">
            <div className="basis-1/2 p-6">
                <CreateBatch />
            </div>
            <div className="border-l-2 border-slate-200 w-min h-screen"></div>
            <div className="basis-1/2">
                <GetBatches />
            </div>

        </div>
    )
}