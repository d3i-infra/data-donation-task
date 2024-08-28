from abc import ABCMeta, abstractmethod

import port.validate as validate

from port.platforms import youtube


class DDLogic(metaclass=ABCMeta):
    
    
    def _file_input(self, extensions, platform):
        description = {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat je opgeslagen hebt op je apparaat. Als je geen {platform} bestand hebt klik dan op “Overslaan” rechts onder."
        }
        return description, extensions

    def _make_visual(self, **kwargs):
        name = kwargs.get("name")

        if name == "wordcloud":
            return {"title": kwargs.get("title"),
                    "type": "wordcloud",
                    "textColumn": kwargs.get("textColumn"),
                    "tokenize": False
                    }
        else:
            return {}
        
    @abstractmethod
    def file_input(self):
        pass

    @abstractmethod
    def validate_zip(self):
        pass
        
    @abstractmethod
    def extract(self):
        pass
        

class DDYoutube(DDLogic):

    def __init__(self, config):
        self.platform = "YouTube"
        self.config = config
        self.extractions = {
            "watch_history": youtube.watch_history_to_df,
        }

    def file_input(self):
        extensions = 'application/zip'
        return self._file_input(extensions, self.platform)
    
    def validate_zip(self, file_prompt_result):
        return youtube.validate_zip(file_prompt_result)
    
    def extract(self, zip_file: str, validation: validate.ValidateInput):
        tables_to_render = []
        for table in self.config.get("tables"):
            extract_fun = self.extractions.get(table.get("name"))
            df = extract_fun(zip_file, validation)
            if not df.empty:
                columns = table.get('columns')
                if columns:
                    df.columns = columns
                visualizations = []
                for visual in table.get("visuals"):
                    visualizations.append(self._make_visual(**visual))
                tables_to_render.append(
                    {
                        "name": "youtube_" + table.get("name"),
                        "df": df,
                        "title": table.get("title"),
                        "description": table.get("description"),
                        "visualizations": visualizations
                    }
                )
        return tables_to_render


class DDFactory:
    
    def __init__(self):
        self._dds = {}

    def register_dd(self, platform, dd):
        self._dds[platform] = dd
    
    def create(self, platform: str, config: dict):
        dd = self._dds.get(platform)
        if not dd:
            raise ValueError(platform)
        return dd(config)
