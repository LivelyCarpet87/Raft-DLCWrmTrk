export type TagInfo = {
  tagName: string;
  tagType: string;
  visible: string;
};

export type ListTagsResponse = {
  tags?: TagInfo[] | null;
};

export type CreateBatchResponse = {
  batchUID: string;
};

export type GetBatcheResponse = {
  creationTime: string;
  batchName: string;
  primaryTag: string;
  secondaryTag: string;
  normMD5: string;
  conditions: string[];
  videoMD5s: string[];
  note: string;
};

export type ListBatchesResponse = {
  batchUIDs: string[];
};

export type TrackletInfo = {
  trackID: string;
  minSpeed: number;
  maxSpeed: number;
  medSpeed: number;
  meanSpeed: number;
  trackLen: number;
  wormLen: number;
  confidence: number;
  warnTxt: string;
};

export type GetVideoResponse = {
  videoName: string;
  numIndv: number;
  uploadTime: string;
  systemMessage: string;
  labeledVideoMD5: string;
  processingStatus: string;
  jobPosition: number;
  tracklets: TrackletInfo[];
};

export type GetNormResponse = {
  processingStatus: string;
  creationTime: string;
  jobPosition: number;
  labeledNormMD5: string;
  normValueAuto: number;
  normValueManual: number;
};

export type GetWorkersStatusResponse = {
  meanJobTime: number;
  numWorkers: number;
  queueLength: number;
};