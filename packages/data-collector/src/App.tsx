import { DataSubmissionPageFactory, ScriptHostComponent } from "@eyra/feldspar";
import { HelloWorldFactory } from "./components/hello_world";
import { ConsentFormVizFactory } from "./factories/consent_form_viz";

function App() {
  return (
    <div className="App">
      <ScriptHostComponent
        workerUrl="./py_worker.js"
        standalone={import.meta.env.DEV}
        factories={[
          new DataSubmissionPageFactory({
            promptFactories: [
                new HelloWorldFactory(),
                new ConsentFormVizFactory(),
            ],
          }),
        ]}
      />
    </div>
  );
}

export default App;
