import { Select, TextInput, Button } from "@mantine/core";
import { useState } from "react";
import { mutate } from "swr";
import { postForm } from "~/apiCaller/apiCaller";

function CreateTagMenu(){    
    const [newTagName, setNewTagName] = useState("");
    const [newTagType, setNewTagType] = useState<string|null>(null);
    async function createTag(){
        if (newTagName.length === 0 || !newTagType) {
            return;
        }
        await postForm('/api/experiment/tags/create', {
            tagName: newTagName,
            tagType: newTagType,
        });

        setNewTagName("");
        setNewTagType(null);
        setTimeout(()=>{
            mutate("/api/experiment/tags/get");
        },2000)
        return;
    }
    return (
        <div className="flex flex-col gap-2 w-72 bg-slate-100 p-2">
            <p>Create Tag</p>
            <TextInput 
                value={newTagName} 
                label="Tag Name" 
                onChange={(event)=>{setNewTagName(event.currentTarget.value)}}
            />
            <Select 
                label="Tag Type" 
                data={["primary", "secondary", "condition"]} 
                value={newTagType}
                onChange={setNewTagType}
            />
            <Button onClick={()=>{createTag()}}>Create Tag</Button>
        </div>
    )
}

export default function ExperimentManagement() {
    return (
        <div className="flex flex-row gap-6 w-full justify-center justify-items-center-safe items-start place-content-around">
            <CreateTagMenu/>
        </div>
    )
}