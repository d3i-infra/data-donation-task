import { DataSubmissionPageFactory, ScriptHostComponent } from "@eyra/feldspar";
import { ConsentFormVizFactory } from "./factories/consent_form_viz";
import { FileInputMultipleFactory } from "./components/file_input_multiple/factory"

function App() {
  return (
    <div className="App">
      <ScriptHostComponent
        workerUrl="./d3i_py_worker.js"
        standalone={import.meta.env.DEV}
        factories={[
          new DataSubmissionPageFactory({
            promptFactories: [
                new ConsentFormVizFactory(),
                new FileInputMultipleFactory(),
            ],
          }),
        ]}
      />
    </div>
  );
}

export default App;
