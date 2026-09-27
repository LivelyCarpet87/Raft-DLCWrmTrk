import { Select, TextInput, Button, Pill, InputBase, Alert } from "@mantine/core";
import { useState } from "react";
import useSWR from "swr";
import { ApiError, fetcher, HttpError, postForm } from "~/apiCaller/apiCaller";
import { HorizontalDivider } from "~/dividers/dividers";
import { mutateUntil } from "~/mutateUntil/mutateUntil";
import type { ListTagsResponse, TagInfo } from "~/types/apiResponses";

type tagManagerProps = {
    tagType: string,
}

function CreateTag({tagType, alertFunc}:{tagType:string, alertFunc:Function}){
     const [newTagName, setNewTagName] = useState("");
    async function createTag(){
        if (newTagName.length === 0) {
            return;
        }
        await postForm('/api/experiment/tags/create', {
            tagName: newTagName,
            tagType: tagType,
        });
    
        const createdTagName = newTagName
        setNewTagName("");
        const isExpected = (tagData:ListTagsResponse) => {
            const secTagList:string[] = tagData?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];
            return secTagList.includes(createdTagName)
        }
        mutateUntil<ListTagsResponse>(`/api/experiment/tags/list?tagType=${tagType}&showHidden=true`, {
            maxAttempts: 3, 
            delayMs: 500, 
            isExpected: isExpected,
        })
        return;
    }
    return (
        <div className="flex flex-row gap-2 flex-nowrap items-end">
            <TextInput 
                value={newTagName} 
                label="New Tag Name" 
                className="grow"
                onChange={(event)=>{setNewTagName(event.currentTarget.value)}}
            />
            <Button 
                classNames={{
                    root: "w-fit !bg-green-600",
                }}
                leftSection={
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v6m3-3H9m12 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
                    </svg>
                }
                onClick={()=>{createTag()}}
            >Create Tag</Button>
        </div>
    )
}

function ManageTag({tagType, alertFunc}:{tagType:string, alertFunc:Function}){
    const { data:existingTags } = useSWR<ListTagsResponse>(
        `/api/experiment/tags/list?tagType=${tagType}&showHidden=true`,
        fetcher
    );
    const tagList:string[] = existingTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];
    const [selectedTagName, setSelectedTagName] = useState<string|null>(null);
    const tagVisible = (existingTags?.tags?.find( (t:TagInfo) => t.tagName == selectedTagName )?.visible ?? "1") === "1"

    async function toggleTagVisibility(){
        if (selectedTagName === null || selectedTagName.length === 0) {
            return;
        }
        try {
            await postForm('/api/experiment/tags/modify', {
                tagName: selectedTagName,
                tagType: tagType,
                tagVisible: (!tagVisible).toString(),
            });
        } catch (err) {
            if (err instanceof ApiError){
                alertFunc(err.info.code + ": " + err.info.message);
                return;
            }
        } finally {
            setSelectedTagName(null);
        }
        
    
        const targetTagName = selectedTagName;
        const targetVisibility = !tagVisible
        const isExpected = (tagData:ListTagsResponse) => {
            return tagData?.tags?.some( 
                (t:TagInfo) => t.tagName == targetTagName && t.visible == (targetVisibility).toString()
            ) ?? false;
        }
        mutateUntil<ListTagsResponse>(`/api/experiment/tags/list?tagType=${tagType}&showHidden=true`, {
            maxAttempts: 3, 
            delayMs: 500, 
            isExpected: isExpected,
        })
        return;
    }

    async function rmTag(){
        if (selectedTagName === null || selectedTagName.length === 0) {
            return;
        }
        
        try {
            await postForm('/api/experiment/tags/rm', {
                tagName: selectedTagName,
                tagType: tagType,
            });
        } catch (err) {
            if (err instanceof ApiError){
                alertFunc(err.info.code + ": " + err.info.message);
                return;
            }
        } finally {
            setSelectedTagName(null);
        }
    
        const targetTagName = selectedTagName;
        const targetVisibility = tagVisible
        setSelectedTagName(null);
        const isExpected = (tagData:ListTagsResponse) => {
            return tagData?.tags?.some( 
                (t:TagInfo) => t.tagName == targetTagName && t.visible == targetVisibility.toString()
            ) ?? false;
        }
        mutateUntil<ListTagsResponse>(`/api/experiment/tags/list?tagType=${tagType}&showHidden=true`, {
            maxAttempts: 3, 
            delayMs: 500, 
            isExpected: isExpected,
        })
        return;
    }

    return (
        <div className="flex flex-col gap-2 flex-nowrap">
            <Select 
                value={selectedTagName} 
                data={tagList}
                placeholder="Select a tag"
                clearable
                allowDeselect
                label="Modify Existing Tag"
                onChange={setSelectedTagName}
                searchable
            />
            <div className="flex flex-row gap-2 flex-nowrap w-full" >
                <Button 
                    classNames={{
                        root: "grow-1 !bg-slate-600",
                    }}
                    leftSection={
                        true ? 
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 0 1 0-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178Z" />
                            <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
                        </svg>
                        :
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M3.98 8.223A10.477 10.477 0 0 0 1.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.451 10.451 0 0 1 12 4.5c4.756 0 8.773 3.162 10.065 7.498a10.522 10.522 0 0 1-4.293 5.774M6.228 6.228 3 3m3.228 3.228 3.65 3.65m7.894 7.894L21 21m-3.228-3.228-3.65-3.65m0 0a3 3 0 1 0-4.243-4.243m4.242 4.242L9.88 9.88" />
                        </svg>
                    }
                    onClick={()=>{toggleTagVisibility()}}
                >{tagVisible ? "Hide" : "Show"} Tag</Button>
                <Button 
                    classNames={{
                        root: "grow-1 !bg-red-600",
                    }}
                    leftSection={
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" />
                        </svg>
                    }
                    onClick={()=>{rmTag()}}
                    disabled={tagVisible}
                >Delete Tag</Button>
            </div>
            
        </div>
    )
}

export function TagManager({tagType}:tagManagerProps){    
    const { data:existingTags } = useSWR<ListTagsResponse>(
        `/api/experiment/tags/list?tagType=${tagType}&showHidden=true`,
        fetcher
    );
    const visibleTagPills = existingTags?.tags?.filter( (tag:TagInfo) => tag.visible == "1" ).map( 
        (tag:TagInfo)=> <Pill 
        key={tag.tagName}
        classNames={{
            root: "!bg-slate-300"
        }}
        >
            {tag.tagName}
        </Pill>
    ) ?? [];
    const hiddenTagPills = existingTags?.tags?.filter( (tag:TagInfo) => tag.visible == "0" ).map( 
        (tag:TagInfo)=> <Pill 
        key={tag.tagName}
        classNames={{
            root: "!bg-slate-300"
        }}
        >
            {tag.tagName}
        </Pill>
    ) ?? [];

    const [alertMsg, setAlertMsg] = useState(null)

    return (
        <div className="flex flex-col gap-2 w-96 bg-slate-100 p-2">
            <h3 className="font-bold text-lg">
                {tagType.charAt(0).toUpperCase() + tagType.slice(1)} Tags
            </h3>
            <InputBase component="div" multiline label="Visible">
                <Pill.Group>{visibleTagPills}</Pill.Group>
            </InputBase>
            <InputBase component="div" multiline label="Hidden">
                <Pill.Group>{hiddenTagPills}</Pill.Group>
            </InputBase>
            <HorizontalDivider />
            <CreateTag tagType={tagType} alertFunc={setAlertMsg}/>
            <HorizontalDivider />
            <ManageTag tagType={tagType} alertFunc={setAlertMsg} />
            {
                alertMsg ?
                <Alert
                    icon={
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z" />
                        </svg>
                    }
                    title="Error"
                    withCloseButton
                    onClose={()=>setAlertMsg(null)}
                >{alertMsg}</Alert>
                :
                <></>
            }
        </div>
    )
}