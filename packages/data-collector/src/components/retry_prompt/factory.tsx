import { 
    PromptFactory,
    ReactFactoryContext 
} from "@eyra/feldspar"
import { RetryPrompt } from "./retry_prompt"
import { PropsUIPromptRetry } from "./types"

export class RetryPromptFactory implements PromptFactory {
  create(body: unknown, context: ReactFactoryContext) {
    if (this.isBody(body)) {
      return <RetryPrompt {...body} {...context} />;
    } 
    return null;
  }

  private isBody(body: unknown): body is PropsUIPromptRetry  {
    return (
      (body as PropsUIPromptRetry).__type__ === "PropsUIPromptRetry"
    );
  }
}

