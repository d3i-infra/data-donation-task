import { DataSubmissionPageFactory, ScriptHostComponent } from "@eyra/feldspar";
import { ConsentFormVizFactory } from "./factories/consent_form_viz";
import { FileInputFactory } from "./components/file_input/factory"
import { ErrorPageFactory } from "./components/error_page/factory"
import { QuestionnaireFactory } from "./components/questionnaire/factory"
import { RetryPromptFactory } from "./components/retry_prompt/factory"

function App() {
  return (
    <div className="App">
      <ScriptHostComponent
        workerUrl="./py_worker.js"
        standalone={import.meta.env.DEV}
        logLevel={import.meta.env.DEV ? "debug" : "info"}
        factories={[
          new DataSubmissionPageFactory({
            promptFactories: [
                new ConsentFormVizFactory(),
                new FileInputFactory(),
                new ErrorPageFactory(),
                new QuestionnaireFactory(),
                new RetryPromptFactory(),
            ],
          }),
        ]}
      />
    </div>
  );
}

export default App;
