from abc import ABC, abstractmethod

class Crawler(ABC):
    @abstractmethod
    def get_data(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

class PublicSource(ABC):
    @abstractmethod
    def extract(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

    @abstractmethod
    def transform(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass

    @abstractmethod
    def load(self) -> None:
        """
        Abstract method that is implemented in classes that inherit it
        """
        pass