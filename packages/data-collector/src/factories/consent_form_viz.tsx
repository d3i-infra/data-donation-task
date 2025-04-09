import { PromptFactory, ReactFactoryContext } from "@eyra/feldspar"
import { ConsentFormViz } from "../components/consent_form_viz/consent_form_viz"
import { PropsUIPromptConsentFormViz } from "../components/consent_form_viz/types";

export class ConsentFormVizFactory implements PromptFactory {
  create(body: unknown, context: ReactFactoryContext) {
    if (this.isBody(body)) {
      return <ConsentFormViz {...body} {...context} />;
    }
    return null;
  }

  private isBody(body: unknown): body is PropsUIPromptConsentFormViz{
    return (
      (body as PropsUIPromptConsentFormViz).__type__ === "PropsUIPromptConsentFormViz"
    );
  }
}
