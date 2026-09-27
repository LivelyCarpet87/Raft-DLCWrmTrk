import { Pill } from "@mantine/core";
import { TagManager } from "./tag-manager";
import { HorizontalDivider } from "~/dividers/dividers";


export default function ExperimentManagement() {
    return (
        <div className="flex flex-col gap-2">
            <h3 className="font-bold text-xl">Tag Management:</h3>
            <p>All available tags in the system are listed here by tag type and visibility. Hidden tags cannot be used when creating new batches. ONLY Hidden Tags that are not associated with experiment data can be deleted.</p>
            <div className="flex flex-row gap-6 w-full justify-center justify-items-center-safe items-start place-content-around">
                <TagManager tagType={"primary"}/>
                <TagManager tagType={"secondary"}/>
                <TagManager tagType={"condition"}/>
            </div>
            <HorizontalDivider />
        </div>
    )
}