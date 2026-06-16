import {
    PromptFactory,
    ReactFactoryContext
} from "@eyra/feldspar"
import { FileInput } from "./file_input"
import { PropsUIPromptFileInputD3I } from "./types"

export class FileInputFactory implements PromptFactory {
  create(body: unknown, context: ReactFactoryContext) {
    if (this.isBody(body)) {
      return <FileInput {...body} {...context} />;
    }
    return null;
  }

  private isBody(body: unknown): body is PropsUIPromptFileInputD3I {
    return (body as PropsUIPromptFileInputD3I).__type__ === "PropsUIPromptFileInputD3I"
  }
}
