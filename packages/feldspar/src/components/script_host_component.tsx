import { useEffect, useRef } from "react";
import Assembly from "../framework/assembly";
import { Bridge } from "../framework/types/modules";
import { LiveBridge } from "../live_bridge";
import FakeBridge from "../fake_bridge";
import React from "react";
import {
  VisualizationProvider,
  useVisualization,
} from "../framework/visualization/react/context";
import { PageFactory } from "../framework/visualization/react/factories/base";
import { LogLevel } from "../framework/logging";
import { Translator } from "../framework/translator";

export interface ScriptHostProps {
  workerUrl: string;
  locale?: string;
  defaultLocale?: string;
  standalone?: boolean;
  className?: string;
  factories?: PageFactory[];
  logLevel?: LogLevel;
  platform?: string;
  mapLocale?: (requested: string) => string;
}

const FeldsparContent: React.FC<ScriptHostProps> = ({
  workerUrl,
  locale = "en",
  defaultLocale,
  standalone = false,
  className,
  factories = [],
  logLevel = "info",
  platform,
  mapLocale,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const assemblyRef = useRef<Assembly | null>(null);
  const workerRef = useRef<Worker | null>(null);
  const { setState, state } = useVisualization();

  useEffect(() => {
    if (!containerRef.current) return;

    const worker = new Worker(workerUrl);
    workerRef.current = worker;

    const run = (bridge: Bridge, selectedLocale: string = locale) => {
      if (defaultLocale != null) Translator.setDefaultLocale(defaultLocale);
      const effectiveLocale = mapLocale != null ? mapLocale(selectedLocale) : selectedLocale;
      const assembly = new Assembly(worker, bridge, effectiveLocale, factories, logLevel, platform);
      assembly.visualizationEngine.start(
        containerRef.current!,
        effectiveLocale,
        setState
      );
      assembly.processingEngine.start();
      assemblyRef.current = assembly;
    };

    if (!standalone && process.env.NODE_ENV === "production") {
      console.log("Initializing bridge system");
      LiveBridge.create(window, run);
    } else {
      console.log("Running with fake bridge");
      run(new FakeBridge());
    }

    const observer = new ResizeObserver(() => {
      const height = window.document.documentElement.getBoundingClientRect().height;
      window.parent.postMessage({ action: "resize", height }, "*");
    });

    observer.observe(window.document.body);

    // Send a message to the parent window indicating that the app has loaded. This is used
    // to trigger the setup of the channel between the iframe and the parent window.
    window.parent.postMessage({ action: 'app-loaded' }, '*')


    return () => {
      observer.disconnect();
      setTimeout(() => {
        assemblyRef.current?.visualizationEngine.terminate();
        assemblyRef.current?.processingEngine.terminate();
        if (workerRef.current) {
          workerRef.current.terminate();
          workerRef.current = null;
        }
      }, 0);
    };
  }, [workerUrl, locale, defaultLocale, standalone, setState, factories, logLevel, platform, mapLocale]);

  return (
    <div ref={containerRef} className={className}>
      {state.elements}
    </div>
  );
};

export const ScriptHostComponent: React.FC<ScriptHostProps> = (props) => (
  <VisualizationProvider>
    <FeldsparContent {...props} />
  </VisualizationProvider>
);
