import {TextInput, FileInput, Select, MultiSelect, Textarea, 
    FileButton, Button, ActionIcon, NumberInput, Progress,
    Notification
} from "@mantine/core"
import useSWR from "swr"
import {useState} from "react"
import { ApiError, fetcher, HttpError, postMultipart } from "~/apiCaller/apiCaller"
import { HorizontalDivider } from "~/dividers/dividers"
import type { CreateBatchResponse, ListTagsResponse, TagInfo } from "~/types/apiResponses"

interface VideoFilePropsType {
    videoID: string
    fileName: string
    numWorms: string|number
    onDelete:(videoID: string)=>void, 
    onWormCountChange:(videoID:string, n:number)=>void
}

type VideoFileInfo = {
    file: File,
    numWorms: string|number
}

function VideoFile({videoID, fileName, numWorms, onDelete, onWormCountChange}:VideoFilePropsType){
    return (
        <div className="flex flex-row flex-nowrap items-center gap-2 justify-between bg-slate-200 border-2 border-slate-300 rounded-md p-2 w-full">
            <p className="font-bold text-left overflow-hidden text-ellipsis text-nowrap inline-block align-middle basis-1/2 max-w-1/2">
                {fileName}
            </p>
            <div className="h-6 border-l-2 border-slate-300 w-fit" />
            <span className="text-md font-bold text-left text-nowrap inline-block align-middle">
                Worm Count:
            </span>
            <NumberInput 
                value={numWorms}
                onChange={(val:number)=>{onWormCountChange(videoID, val)}}
                className="w-16"
                min={1}
                max={99}
                clampBehavior="strict"
            />
            <ActionIcon 
            size={42}
            onClick={(_event:any)=>{onDelete(videoID)}}
            classNames={{
                root: "align-middle !bg-red-700",
            }}>
                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                    <path strokeLinecap="round" strokeLinejoin="round" d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0" />
                </svg>
            </ActionIcon>
        </div>
    )
}

export default function CreateBatch() {
    const { data:primTags } = useSWR<ListTagsResponse>(
        "/api/experiment/tags/list?tagType=primary",
        fetcher
    );
    const { data:secTags } = useSWR<ListTagsResponse>(
        "/api/experiment/tags/list?tagType=secondary",
        fetcher
    );
    const { data:condTags } = useSWR<ListTagsResponse>(
        "/api/experiment/tags/list?tagType=condition",
        fetcher
    );

    const [batchName, setBatchName] = useState<string>("");
    const [normFile, setNormFile] = useState<File|null>(null);

    const [primTag, setPrimTag] = useState<string|null>(null);
    const [secTag, setSecTag] = useState<string|null>(null);
    const [conditions, setConditions] = useState<string[]>([]);
    const [note, setNote] = useState<string>("");

    const [videoFilesInfo, setVideoFilesInfo] = useState<Map<string, VideoFileInfo>>(
        new Map<string, VideoFileInfo>()
    );

    const [uploading, setUploading] = useState<boolean>(false)
    const [progress, setProgress] = useState<number>(0)
    const [errorText, setErrorText] = useState<string>('')

    function addVideos(newFiles:File[]){
        setVideoFilesInfo(
            (videoFilesInfo) => {
                const existingNames = new Set(
                    Array.from(videoFilesInfo.values()).map((fileInfo:VideoFileInfo) => fileInfo.file.name)
                );

                const uniqueNewFiles = newFiles.filter(
                    (file) => !existingNames.has(file.name)
                );
                

                const next = new Map(videoFilesInfo)
                uniqueNewFiles.map(
                    (file:File) => {next.set(crypto.randomUUID(), ({
                        file: file,
                        numWorms: ''
                    }))}
                );
                return next;
            }
        )
    }
    function onWormCountChange(videoID:string, val:number){
         setVideoFilesInfo(
            (videoFilesInfo) => {
                const next = new Map(videoFilesInfo);
                const oldInfo:VideoFileInfo = next.get(videoID)!
                oldInfo.numWorms = val
                next.set(videoID,oldInfo)
                return next;
            }
        );
    }
    function onDeleteVideo(videoID:string){
        setVideoFilesInfo(
            (videoFilesInfo) => {
                const next = new Map(videoFilesInfo);
                next.delete(videoID)
                return next;
            }
        );
    }

    async function submitBatch() {
        if (batchName.length == 0) {
            setErrorText("Batch Name cannot be empty.");
            return;
        } else if (normFile === null) {
            setErrorText("Normalizer Image not selected.");
            return;
        } else if (primTag === null) {
            setErrorText("Primary Tag not selected.");
            return;
        } else if (secTag === null) {
            setErrorText("Secondary Tag not selected.");
            return;
        } else if (conditions.length == 0){
            setErrorText("Must specify at least one experiment condition.");
            return;
        } else if (videoFilesInfo.size == 0){
            setErrorText("Must select at least one video file.");
            return;
        }
        for (const [videoID, fileInfo] of videoFilesInfo.entries()){
            if (fileInfo.numWorms === ''){
                setErrorText(`Must specify number of worms that appear in ${fileInfo.file.name}`);
                return;
            } else if (fileInfo.file.size > 40 * 1024 * 1024){
                setErrorText(`File is too large: ${fileInfo.file.name}`);
                return;
            } else if (fileInfo.file.size < 5 * 1024 * 1024){
                setErrorText(`File is too small: ${fileInfo.file.name}`);
                return;
            }
            try {
                await fileInfo.file.slice(0, 1).arrayBuffer();
            } catch {
                setErrorText(`File is no longer accessible: ${fileInfo.file.name}`);
                return
            }
        }
        setErrorText("")
        setUploading(true)

        const form = new FormData();

        form.append("primaryTag", primTag);
        form.append("secondaryTag", secTag);
        form.append("conditions", JSON.stringify(conditions));
        form.append("batchName", batchName);
        form.append("note", note.trim());
        form.append("normFile", normFile);

        let resp:CreateBatchResponse|undefined
        try {
            resp = await postMultipart<CreateBatchResponse>("/api/experiment/batches/create",form)
        } catch (err) {
             if (err instanceof ApiError) {
                setErrorText(err.info.message)
                setUploading(false)
                return
            } else if (err instanceof HttpError) {
                console.error(`HTTP error: ${err.status}`);
                setErrorText("Unexpected HTTP error occurred")
                setUploading(false)
                return
            } else {
                console.error("Unknown error", err);
                setErrorText("Unexpected error occurred")
                setUploading(false)
                return
            }
        }
        const batchUID = resp!.batchUID
        setProgress(100*1/(1+videoFilesInfo.size))
        for (const [_videoID, fileInfo] of videoFilesInfo.entries()) {
            const form = new FormData();

            form.append("batchUID", batchUID);
            form.append("videoFile", fileInfo.file);
            form.append("numIndv", String(fileInfo.numWorms));

            try {
                await postMultipart<undefined>("/api/experiment/videos/upload",form)
            } catch (err) {
                if (err instanceof ApiError) {
                    setErrorText(err.info.message)
                    setUploading(false);
                    return;
                } else if (err instanceof HttpError) {
                    console.error(`HTTP error: ${err.status}`);
                    setErrorText("Unexpected HTTP error occurred");
                    setUploading(false);
                    return;
                } else {
                    console.error("Unknown error", err);
                    setErrorText("Unexpected error occurred");
                    setUploading(false);
                    return;
                }
            }
            setProgress(progress+100*1/(1+videoFilesInfo.size))
        }
        setBatchName("");
        setNormFile(null);
        setConditions([]);
        setNote("");
        setVideoFilesInfo(new Map());
        setUploading(false);

    }

    const primTagList:string[] = primTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];
    const secTagList:string[] = secTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];
    const condTagList:string[] = condTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];

    return (
        <div className="flex flex-col gap-6 max-w-xl">
            <div className="flex flex-row gap-6 w-full flex-nowrap">
                <TextInput
                label="Batch Name" 
                description="Name of this batch of videos for future identification."
                className="basis-3/5 max-w-3/5"
                placeholder="Batch Name"
                value={batchName}
                onChange={(event:any)=>{setBatchName(event.currentTarget.value)}}
                    >
                </TextInput>
                <FileInput 
                    label="Normalizer Image"
                    description="Image used to convert from px to mm."
                    placeholder="Select a PNG image"
                    accept="image/png"
                    clearable
                    className="basis-2/5 max-w-2/5"
                    value={normFile}
                    onChange={setNormFile}
                />
            </div>
            <HorizontalDivider />
            <div className="flex flex-col gap-2">
                <div className="flex flex-row gap-2 w-full flex-nowrap">
                    <Select 
                        label="Primary Tag"
                        data={primTagList}
                        searchable
                        nothingFoundMessage="No matching tags..."
                        className="basis-1/2"
                        value={primTag}
                        onChange={setPrimTag}
                    />
                    <Select 
                        label="Secondary Tag"
                        data={secTagList}
                        searchable
                        nothingFoundMessage="No matching tags..."
                        className="basis-1/2"
                        value={secTag}
                        onChange={setSecTag}
                    />
                </div>
                <MultiSelect 
                    label="Experiment Conditions"
                    description="Select ALL experiment conditions that apply to this batch of videos."
                    data={condTagList}
                    searchable
                    nothingFoundMessage="No matching experiment conditions..."
                    className="w-full"
                    value={conditions}
                    onChange={setConditions}
                />
                <Textarea 
                    label="Notes"
                    description="Any notes about this batch of videos. (Optional)"
                    placeholder="Record any experimental anomalies or other information here."
                    autosize
                    className="w-full"
                    minRows={2}
                    maxRows={5}
                    value={note}
                    onChange={(event:any) => setNote(event.currentTarget.value)}
                />
            </div>
            <HorizontalDivider />
            <div className="flex flex-col gap-2 w-full flex-nowrap">
                {Array.from(videoFilesInfo.entries()).map(
                    ([videoID, fileInfo]) => (
                        <VideoFile 
                        key={videoID} 
                        videoID={videoID} 
                        fileName={fileInfo.file.name} 
                        numWorms={fileInfo.numWorms} 
                        onDelete={onDeleteVideo} 
                        onWormCountChange={onWormCountChange}/>
                    )
                )}
            </div>
            <div className="flex flex-col gap-2">
                <FileButton onChange={addVideos} accept="video/mp4" multiple disabled={uploading}>
                    {(props: any) => <Button {...props}>Add Videos</Button>}
                </FileButton>
                <p className="text-slate-400 text-sm font-thin">Video files will not be uploaded until the entire batch is submitted.</p>
            </div>
            <HorizontalDivider />
            {uploading ? <Progress value={progress} animated /> : null }
            
            {
                errorText ? 
                <Notification color="red" title="Error" onClose={()=>{setErrorText("")}}>
                    {errorText}
                </Notification>
                : null
            }
            
            <Button classNames={{
                        root: "text-nowrap !bg-green-600",
                    }} disabled={uploading} onClick={(_event)=>{submitBatch()}}>
                Submit Batch
            </Button>
        </div>
    )
}