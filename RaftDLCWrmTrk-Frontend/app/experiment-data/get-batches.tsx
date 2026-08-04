import {TextInput, FileInput, Select, MultiSelect, Textarea, 
    FileButton, Button, ActionIcon, NumberInput, Progress,
    Notification, Table, CopyButton, Tooltip
} from "@mantine/core"
import useSWR from "swr"
import {mutate} from "swr"
import {useEffect, useState} from "react"
import { ApiError, fetcher, generateURL, HttpError, postForm, postMultipart } from "~/apiCaller/apiCaller"
import { HorizontalDivider, VerticalDivider } from "~/dividers/dividers";
import type { GetBatcheResponse, GetNormResponse, GetVideoResponse, ListBatchesResponse, TagInfo, TrackletInfo } from "~/types/apiResponses";

const symbols = {
    "GOOD": (
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75 11.25 15 15 9.75M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
        </svg>
    ),
    "WARN": (
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z" />
        </svg>
    ),
    "FAIL": (
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="m9.75 9.75 4.5 4.5m0-4.5-4.5 4.5M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
        </svg>
    ),
    "PEND": (
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
            <path strokeLinecap="round" strokeLinejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99" />
        </svg>
    ),
}

function toNormNumber(value: string | number): number {
  if (typeof value === "string" && value.trim() === "") {
    return -1;
  }

  const num = Number(value);

  if (!Number.isFinite(num) || num <= 0) {
    return -1;
  }

  return num;
}

type renderedTrackletInfo = {
    trackID: string,
    convMeanSpeed: number,
    confidence: number,
    warnTxt: string,
}

function VideoBox({batchUID, videoMD5, conversion}:{batchUID: string,videoMD5:string, conversion:number}) {
    const { data:getVideoResp } = useSWR<GetVideoResponse>(
        `/api/experiment/videos/get?batchUID=${encodeURIComponent(batchUID)}&srcVideoMD5=${encodeURIComponent(videoMD5)}`,
        fetcher
    );
    const [collapse, setCollapse] = useState(false)
    const [renderedInfo, setRenderedInfo] = useState<renderedTrackletInfo[]>([])
    const [hideLowConfidence, setHideLowConfidence] = useState(true)
    useEffect(
        () => {
            const rows:renderedTrackletInfo[] = (getVideoResp?.tracklets??[])
            .filter(
                (tracklet:TrackletInfo) => {
                    return !hideLowConfidence || tracklet.confidence > 0.6;
                }
            )
            .map(
                (tracklet:TrackletInfo) => ({
                trackID:tracklet.trackID,
                convMeanSpeed: (conversion > 0 ? tracklet.meanSpeed * conversion : tracklet.meanSpeed).toPrecision(3),
                confidence: (tracklet.confidence*100).toPrecision(3),
                warnTxt: tracklet.warnTxt,
            }));
            setRenderedInfo(rows);
        },
        [getVideoResp, hideLowConfidence, conversion]
    )

    const rows = renderedInfo.map((tracklet:renderedTrackletInfo) => (
        <Table.Tr key={tracklet.trackID}>
        <Table.Td>{tracklet.trackID}</Table.Td>
        <Table.Td>{tracklet.convMeanSpeed}</Table.Td>
        <Table.Td>{tracklet.confidence}%</Table.Td>
        <Table.Td>
            <Tooltip label={tracklet.warnTxt}>
                {tracklet.warnTxt.startsWith("WARNING") ? symbols.WARN : symbols.GOOD}
            </Tooltip>
        </Table.Td>
        </Table.Tr>
    ));
    const tsvData = renderedInfo
        .map(
            (tracklet:renderedTrackletInfo) => (
                `${tracklet.trackID}\t${tracklet.convMeanSpeed}\t${tracklet.confidence}%`
            )
        )
        .join("\n");

    let resTable = <></>
    if (renderedInfo.length > 0) {
        resTable = (
            <Table striped withTableBorder withColumnBorders>
                <Table.Thead>
                    <Table.Tr>
                    <Table.Th>Indv</Table.Th>
                    <Table.Th>Mean Speed ({conversion > 0 ? "mm" : "px"}/sec)</Table.Th>
                    <Table.Th>Confidence</Table.Th>
                    <Table.Th>Status</Table.Th>
                    </Table.Tr>
                </Table.Thead>
                <Table.Tbody>{rows}</Table.Tbody>
                <Table.Caption>Hover to see explanations.</Table.Caption>
            </Table>
        );
    } else {
        resTable = (<p>No tracklets...</p>);
    }

    let videoStateSymbol = null;
    if (getVideoResp?.processingStatus === "pending") {
        videoStateSymbol = symbols.PEND;
    } else if (getVideoResp?.processingStatus === "assigned") {
        videoStateSymbol = symbols.PEND;
    } else if (getVideoResp?.processingStatus === "done") {
        if (getVideoResp?.systemMessage.startsWith("INFO")){
            videoStateSymbol = symbols.GOOD;
        } else {
            videoStateSymbol = symbols.WARN;
        }
    } else if (getVideoResp?.processingStatus === "failed") {
        videoStateSymbol = symbols.FAIL;
    } else if (getVideoResp?.processingStatus === "crashed") {
        videoStateSymbol = symbols.FAIL;
    }
    
    return (
        <div className="flex flex-col p-6 gap-4 border-2 border-slate-300 rounded-md">
            <div className="flex flex-row gap-6 w-full flex-nowrap">
                <p className="grow font-bold text-lg">{getVideoResp?.videoName??"Loading..."}</p>
                <ActionIcon 
                variant="transparent" 
                className={collapse ? "rotate-90" : ""}
                onClick={()=>{setCollapse(!collapse)}}>
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" />
                    </svg>
                </ActionIcon>
            </div>
            <div className={collapse ? "hidden" : "flex flex-col gap-4"}>
            <HorizontalDivider />
            <div className="flex flex-row gap-6 w-full flex-nowrap">
                {videoStateSymbol}
                <p className="grow">{getVideoResp?.systemMessage??"Loading..."}</p>
                <ActionIcon 
                    size={32}
                    hidden={renderedInfo.length===(getVideoResp?.tracklets??[]).length && hideLowConfidence}
                    onClick={()=>{setHideLowConfidence(!hideLowConfidence)}}
                    classNames={{
                        root: "align-middle !bg-slate-700",
                    }}>
                    {hideLowConfidence ? 
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M3.98 8.223A10.477 10.477 0 0 0 1.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.451 10.451 0 0 1 12 4.5c4.756 0 8.773 3.162 10.065 7.498a10.522 10.522 0 0 1-4.293 5.774M6.228 6.228 3 3m3.228 3.228 3.65 3.65m7.894 7.894L21 21m-3.228-3.228-3.65-3.65m0 0a3 3 0 1 0-4.243-4.243m4.242 4.242L9.88 9.88" />
                        </svg>
                    :
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 0 1 0-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178Z" />
                            <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 1 1-6 0 3 3 0 0 1 6 0Z" />
                        </svg>
                    }
                </ActionIcon>
            </div>
            {getVideoResp?.processingStatus === "done" 
            || getVideoResp?.processingStatus === "failed"
            || getVideoResp?.processingStatus === "crashed" ?
                resTable : <></>
            }
            <HorizontalDivider />
            <div className="flex flex-row gap-6 w-full flex-nowrap">
                <CopyButton value={tsvData}>
                    {({ copied, copy }) => (
                        <Button 
                            className="grow"
                            color={copied ? 'teal' : 'blue'} 
                            onClick={copy}
                            rightSection={
                                <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 0 0 2.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 0 0-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 0 0 .75-.75 2.25 2.25 0 0 0-.1-.664m-5.8 0A2.251 2.251 0 0 1 13.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25ZM6.75 12h.008v.008H6.75V12Zm0 3h.008v.008H6.75V15Zm0 3h.008v.008H6.75V18Z" />
                                </svg>
                            }
                            disabled={renderedInfo.length == 0}
                        >
                        {copied ? 'Copied Data' : 'Copy Data'}
                        </Button>
                    )}
                </CopyButton>
                <Button 
                    rightSection={
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 0 1-1.125-1.125M3.375 19.5h1.5C5.496 19.5 6 18.996 6 18.375m-3.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-1.5A1.125 1.125 0 0 1 18 18.375M20.625 4.5H3.375m17.25 0c.621 0 1.125.504 1.125 1.125M20.625 4.5h-1.5C18.504 4.5 18 5.004 18 5.625m3.75 0v1.5c0 .621-.504 1.125-1.125 1.125M3.375 4.5c-.621 0-1.125.504-1.125 1.125M3.375 4.5h1.5C5.496 4.5 6 5.004 6 5.625m-3.75 0v1.5c0 .621.504 1.125 1.125 1.125m0 0h1.5m-1.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m1.5-3.75C5.496 8.25 6 7.746 6 7.125v-1.5M4.875 8.25C5.496 8.25 6 8.754 6 9.375v1.5m0-5.25v5.25m0-5.25C6 5.004 6.504 4.5 7.125 4.5h9.75c.621 0 1.125.504 1.125 1.125m1.125 2.625h1.5m-1.5 0A1.125 1.125 0 0 1 18 7.125v-1.5m1.125 2.625c-.621 0-1.125.504-1.125 1.125v1.5m2.625-2.625c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125M18 5.625v5.25M7.125 12h9.75m-9.75 0A1.125 1.125 0 0 1 6 10.875M7.125 12C6.504 12 6 12.504 6 13.125m0-2.25C6 11.496 5.496 12 4.875 12M18 10.875c0 .621-.504 1.125-1.125 1.125M18 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m-12 5.25v-5.25m0 5.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125m-12 0v-1.5c0-.621-.504-1.125-1.125-1.125M18 18.375v-5.25m0 5.25v-1.5c0-.621.504-1.125 1.125-1.125M18 13.125v1.5c0 .621.504 1.125 1.125 1.125M18 13.125c0-.621.504-1.125 1.125-1.125M6 13.125v1.5c0 .621-.504 1.125-1.125 1.125M6 13.125C6 12.504 5.496 12 4.875 12m-1.5 0h1.5m-1.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M19.125 12h1.5m0 0c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h1.5m14.25 0h1.5" />
                        </svg>
                    }
                    disabled={!getVideoResp?.labeledVideoMD5}
                    onClick={()=>{
                        if (getVideoResp){
                            window.open(generateURL(`/api/filer/${getVideoResp.labeledVideoMD5}`),"_blank")
                        }}}
                >View Labeled</Button>
                <Button 
                    rightSection={
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 0 0 5.25 21h13.5A2.25 2.25 0 0 0 21 18.75V16.5M16.5 12 12 16.5m0 0L7.5 12m4.5 4.5V3" />
                        </svg>
                    }
                    onClick={()=>{window.open(generateURL(`/api/filer/${videoMD5}`),"_blank")}}
                >Original</Button>
            </div>
            </div>
        </div>
    );
}

function BatchBox({batchUID}:{batchUID: string}) {
    const { data:getBatchResp } = useSWR<GetBatcheResponse>(
        `/api/experiment/batches/get?batchUID=${encodeURIComponent(batchUID)}`,
        fetcher
    );
    const { data:condTags } = useSWR<ListTagsResponse>(
        "/api/experiment/tags/list?tagType=condition",
        fetcher
    );
    const condTagList:string[] = condTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];

    const { data:normInfo } = useSWR<GetNormResponse>(
        getBatchResp ? `/api/experiment/norms/get?normMD5=${getBatchResp.normMD5}` : null,
        fetcher
    );

    const [collapse, setCollapse] = useState(false);
    const [editMode, setEditMode] = useState(false);
    const [batchName, setBatchName] = useState("Loading...");
    const [normVal, setNormVal] = useState<number|string>("");
    const [conditions, setConditions] = useState<string[]>([]);
    const [note, setNote] = useState("");
    useEffect(
        () => {
            if (editMode) {
                return;
            }
            if (getBatchResp === undefined) {
                return;
            }
            setBatchName(getBatchResp.batchName);
            setConditions(getBatchResp.conditions);
            setNote(getBatchResp.note);
        },
        [getBatchResp]
    )
    useEffect(
        () => {
            if (editMode) {
                return;
            }
            if (normInfo === undefined) {
                return;
            }
            if (normInfo.normValueManual > 0){
                setNormVal(normInfo.normValueManual);
            } else if (normInfo.normValueAuto > 0) {
                setNormVal(normInfo.normValueAuto);
            } else {
                setNormVal("");
            }
            
        },
        [normInfo]
    )

    const videoBoxes = (getBatchResp?.videoMD5s??[]).map((vidMD5) => (
        <VideoBox key={vidMD5} batchUID={batchUID} videoMD5={vidMD5} conversion={toNormNumber(normVal)}/>
    ));

    async function updateBatchInfo(){
        if (getBatchResp === undefined) {
            return;
        }
        await postForm('/api/experiment/norms/set', {
            normMD5: getBatchResp!.normMD5,
            normValueManual: String(toNormNumber(normVal)),
        });
        await postForm('/api/experiment/batches/update', {
            batchUID: batchUID,
            conditions: JSON.stringify(conditions),
            batchName: batchName,
            note: note,
        });

        
        setEditMode(false);
        setTimeout(()=>{
            mutate(`/api/experiment/norms/get?normMD5=${getBatchResp!.normMD5}`);
            mutate(`/api/experiment/batches/get?batchUID=${encodeURIComponent(batchUID)}`);
        },2000)
        return;
    }
    

    return (
        <div className="flex flex-col bg-slate-100 p-6 gap-4 rounded-md">
            <div className="flex flex-row gap-6 w-full flex-nowrap items-center">
                <TextInput
                    className="grow"
                    placeholder="Batch Name"
                    value={batchName}
                    onChange={(event)=>{setBatchName(event.currentTarget.value)}}
                    classNames={editMode ? {} : {
                        input: "!border-0 !bg-inherit !p-0 !text-lg !font-bold !caret-transparent"
                    }}
                        >
                </TextInput>
                {editMode ?
                    <div className="flex flex-row flex-nowrap gap-2">
                        <Button 
                            hidden={collapse}
                            onClick={()=>{setEditMode(false)}}
                            className="!bg-red-700"
                        >Clear</Button>
                        <Button 
                            hidden={collapse}
                            onClick={()=>{updateBatchInfo()}}
                            disabled={batchName.length == 0||conditions.length == 0}
                            className="!bg-green-600"
                        >Save</Button>
                    </div>
                :
                    <Button 
                    hidden={collapse}
                    className="!bg-slate-700"
                    rightSection={
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                            <path strokeLinecap="round" strokeLinejoin="round" d="m16.862 4.487 1.687-1.688a1.875 1.875 0 1 1 2.652 2.652L10.582 16.07a4.5 4.5 0 0 1-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 0 1 1.13-1.897l8.932-8.931Zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0 1 15.75 21H5.25A2.25 2.25 0 0 1 3 18.75V8.25A2.25 2.25 0 0 1 5.25 6H10" />
                        </svg>
                    }
                    onClick={()=>{setEditMode(true)}}
                    >Edit</Button>
                }
                <ActionIcon 
                variant="transparent" 
                className={collapse ? "rotate-90" : ""}
                onClick={()=>{setCollapse(!collapse)}}>
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" />
                    </svg>
                </ActionIcon>
            </div>
            <div className={collapse ? "hidden" : "flex flex-col gap-4"}>
            <HorizontalDivider />
            <div className="flex flex-row gap-2 w-full flex-nowrap items-center h-fit justify-between">
                <div className="flex flex-row  flex-nowrap items-center">
                    <p className="text-lg font-bold mr-2">Normalization:</p>
                    <NumberInput
                        className={editMode ? "w-16 mr-2" :"w-10"}
                        placeholder="0.00"
                        value={normVal}
                        min={0}
                        onChange={setNormVal}
                        rightSection={<></>}
                        classNames={
                            editMode ? {} :
                            {
                                input: "!border-0 !bg-inherit !p-0 !text-lg text-right !caret-transparent"
                            }
                        }
                                >
                    </NumberInput>
                    <p className="text-lg">mm/px</p>
                </div>
                <Button 
                rightSection={
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m2.25 15.75 5.159-5.159a2.25 2.25 0 0 1 3.182 0l5.159 5.159m-1.5-1.5 1.409-1.409a2.25 2.25 0 0 1 3.182 0l2.909 2.909m-18 3.75h16.5a1.5 1.5 0 0 0 1.5-1.5V6a1.5 1.5 0 0 0-1.5-1.5H3.75A1.5 1.5 0 0 0 2.25 6v12a1.5 1.5 0 0 0 1.5 1.5Zm10.5-11.25h.008v.008h-.008V8.25Zm.375 0a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Z" />
                    </svg>
                }
                disabled={!getBatchResp?.normMD5}
                onClick={()=>{window.open(generateURL(`/api/filer/${getBatchResp?.normMD5}`),"_blank")}}
                >Original</Button>
                <Button 
                rightSection={
                    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" className="size-6">
                        <path strokeLinecap="round" strokeLinejoin="round" d="m2.25 15.75 5.159-5.159a2.25 2.25 0 0 1 3.182 0l5.159 5.159m-1.5-1.5 1.409-1.409a2.25 2.25 0 0 1 3.182 0l2.909 2.909m-18 3.75h16.5a1.5 1.5 0 0 0 1.5-1.5V6a1.5 1.5 0 0 0-1.5-1.5H3.75A1.5 1.5 0 0 0 2.25 6v12a1.5 1.5 0 0 0 1.5 1.5Zm10.5-11.25h.008v.008h-.008V8.25Zm.375 0a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Z" />
                    </svg>
                }
                disabled={!normInfo?.labeledNormMD5}
                onClick={()=>{window.open(generateURL(`/api/filer/${normInfo?.labeledNormMD5}`),"_blank")}}
                >Labeled</Button>
            </div>
            <HorizontalDivider />
                <MultiSelect 
                    label="Experiment Conditions"
                    data={condTagList}
                    searchable
                    nothingFoundMessage="No matching experiment conditions..."
                    className="w-full"
                    value={conditions}
                    onChange={setConditions}
                    readOnly={!editMode}
                    rightSection={<div/>}
                    classNames={editMode ? {} : {
                        label: "!font-bold !text-lg",
                        input: "!border-0 !bg-inherit !p-0 !mt-2 !caret-transparent",
                        pill: "!bg-slate-300 !text-lg !m-1",
                    }}
                />
                <HorizontalDivider />
                <Textarea 
                    label="Notes"
                    placeholder="Record any experimental anomalies or other information here."
                    autosize
                    className="w-full"
                    minRows={2}
                    maxRows={5}
                    value={note}
                    onChange={(event)=>{setNote(event.currentTarget.value)}}
                    readOnly={!editMode}
                    classNames={editMode ? {} : {
                        label: "!text-lg !font-bold",
                        input: "!border-0 !bg-inherit !p-0 !caret-transparent"
                    }}
                />
            <HorizontalDivider />
            {videoBoxes}
            </div>
        </div>
    )
}

export default function GetBatches() {
    const { data:primTags } = useSWR<ListTagsResponse>(
        "/api/experiment/tags/list?tagType=primary",
        fetcher
    );
    const { data:secTags } = useSWR<ListTagsResponse>(
        "/api/experiment/tags/list?tagType=secondary",
        fetcher
    );
    const primTagList:string[] = primTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];
    const secTagList:string[] = secTags?.tags?.map( (tag:TagInfo)=>tag.tagName ) ?? [];

    const [primTag, setPrimTag] = useState<string|null>(null);
    const [secTag, setSecTag] = useState<string|null>(null);
    const { data:lsBatchResp } = useSWR<ListBatchesResponse>(
        (primTag != null && secTag != null) ? `/api/experiment/batches/list?primaryTag=${encodeURIComponent(primTag)}&secondaryTag=${encodeURIComponent(secTag)}` : null,
        fetcher
    );

    const batchBoxes = (lsBatchResp?.batchUIDs??[]).map((batchUID) => (
        <BatchBox key={batchUID} batchUID={batchUID} />
    ));

    return (
        <div className="flex flex-col gap-6 max-w-xl">
            <div className="flex flex-row gap-2 w-full flex-nowrap items-end">
                <p className={"font-bold text-left text-nowrap mb-2"}>Filter By: </p>
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
            <HorizontalDivider />
            {batchBoxes}
        </div>
    )
}