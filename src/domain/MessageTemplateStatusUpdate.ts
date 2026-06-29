export interface MessageTemplateStatusUpdate {
  sourceId: string;
  sourceStatus?: string;
  sourceReason?: string;
  status: string;
  notes?: string;
  payload?: any;
}

export default MessageTemplateStatusUpdate;
