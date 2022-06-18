from concurrent.futures import ThreadPoolExecutor
import requests
import json
import time
from itertools import repeat


MAX_QUERIES_PER_SECOND = 5


class PokemonInfoRaw():
    def __init__(self, id, types, stats, moves, species, past_types) -> None:
        self.id = id
        self.types = types
        self.stats = stats
        self.moves = moves
        self.species = species
        self.past_types = past_types


    def __new__(cls, *args, **kwargs):
        instance = super(PokemonInfoRaw, cls).__new__(cls)
        return instance


    def __aggregate_species(species) -> str:
        return species["name"]


    def __aggregate_stats(stats) -> "list[int]":
        return [stat["base_stat"] for stat in stats if "base_stat" in stat]


    def __aggregate_moves(moves) -> "list[str]":
        return [move["move"]["name"] for move in moves if "move" in move and "name" in move["move"] ]
        

    def __aggregate_types(types) -> "list[str]":
        return [type_el["type"]["name"] for type_el in types if "type" in type_el and "name" in type_el["type"]]


    def __get_id(url: str, what_id: str) -> int:
        what_id_pos = url.rfind(f"{what_id}/") + len(f"{what_id}/")
        print(url)
        return int(url[what_id_pos:-1])


    def __aggregate_past_types(past_types) -> "dict(str, list[str])":
        types_by_generation = dict()
        for past_type in past_types:
            last_generation_id = PokemonInfoRaw.__get_id(past_type["generation"]["url"], "generation")
            types = PokemonInfoRaw.__aggregate_types(past_type["types"])
            types_by_generation[last_generation_id] = types

        return types_by_generation


    def create_pokemon_info(self, generations_amount: int):
        species_name = PokemonInfoRaw.__aggregate_species(self.species)
        stats = PokemonInfoRaw.__aggregate_stats(self.stats) 
        moves = PokemonInfoRaw.__aggregate_moves(self.moves)
        types = PokemonInfoRaw.__aggregate_types(self.types)
        types_by_generation = PokemonInfoRaw.__aggregate_past_types(self.past_types)
        types_by_generation[generations_amount] = types
        return PokemonInfo(self.id, species_name, stats, moves, types_by_generation)


    def __str__(self):
        return f"ID: {self.id}\nTypes: {self.types}\nStats: {self.stats}\nMoves: {self.moves}\nSpecies : {self.species}\nPast types: {self.past_types}"


class PokemonInfo():
    def __init__(self, id: int, species_name: str, stats: "list[int]", moves: "list[str]", types_by_generation: "dict(str, list[str])") -> None:
        self.id = id
        self.species = species_name
        self.stats = stats
        self.moves = moves
        self.types_by_generation  = types_by_generation


    def __new__(cls, *args, **kwargs):
        instance = super(PokemonInfo, cls).__new__(cls)
        return instance


    def __str__(self):
        return f"ID: {self.id}\nSpecies: {self.species}\nStats: {self.stats}\nMoves: {self.moves}\nTypes by generation: {self.types_by_generation}\n"


class GenerationInfo():
    def __init__(self, id: int, pokemon_species: "list[str]") -> None:
        self.id = id
        self.pokemon_species = pokemon_species


    def __new__(cls, *args, **kwargs):
        instance = super(GenerationInfo, cls).__new__(cls)
        return instance


    def __str__(self):
        return f"ID: {self.id}\nPokemon species: {self.pokemon_species}\n"


class FullInfo():
    def __init__(self, pokemons_info: "list[PokemonInfo]", generations_info: "list[GenerationInfo]"):
        self.pokemons_info = pokemons_info
        self.generations_info = generations_info


    def __new__(cls, *args, **kwargs):
        instance = super(FullInfo, cls).__new__(cls)
        return instance


    def __str__(self):
        return f"Pokemons info: {self.pokemons_info}\n \
                Generations info: {self.generations_info}\n"



def __get_json_file(url: str, offset: int = 0, limit: int = 1000):
    response = requests.get(f'{url}?offset={offset}&limit={limit}')
    return response.json()


def __get_list(url: str, list_name: str, offset: int) -> "list[str]":
    list_url = f"{url}{list_name}/"
    len_list = __get_json_file(list_url, limit=1)["count"]
    return __get_json_file(list_url, offset=offset, limit=len_list - offset)["results"] #change to len_list !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


def __get_urls_list(raw_list: "list[dict(str, str)]"):
    return [el["url"] for el in raw_list]


def __load_json_file(url: str):
    response = requests.get(url)
    return response.json()


def __get_pokemon_info_from_url(url: str, max_queries_number: int):
    time.sleep(1 / max_queries_number)
    json_file = __load_json_file(url)
    pokemon_id = json_file["id"]
    pokemon_types = json_file["types"]
    pokemon_stats = json_file["stats"]
    pokemon_moves = json_file["moves"]
    pokemon_species = json_file["species"]
    pokemon_past_types = json_file["past_types"]
    return PokemonInfoRaw(pokemon_id, pokemon_types, pokemon_stats, pokemon_moves, pokemon_species, pokemon_past_types)


def __get_generation_info_from_url(url: str, max_queries_number: int):
    time.sleep(1 / max_queries_number)
    json_file = __load_json_file(url)
    generation_id = json_file["id"]
    pokemon_species = [pokemon["name"] for pokemon in json_file["pokemon_species"]]
    return GenerationInfo(generation_id, pokemon_species)


def convert_list_object_to_dict(objects_list):
    return [el.__dict__ for el in objects_list]


def convert_to_json_str(dicts_list):
    return json.dumps(dicts_list)


def copy_info_from_api(url: str, pokemon_offset = 0, generation_offset = 0, type_offset = 0):
    pokemons = __get_list(url, "pokemon", pokemon_offset)
    generations = __get_list(url, "generation", generation_offset)

    pokemons_urls = __get_urls_list(pokemons)
    generations_urls = __get_urls_list(generations)

    with ThreadPoolExecutor(max_workers=2) as executor:
        pokemons_info_raw = executor.map(__get_pokemon_info_from_url, pokemons_urls, repeat(MAX_QUERIES_PER_SECOND))

        generations_amount = len(generations_urls) + generation_offset
        pokemons_info = [pokemon_info_raw.create_pokemon_info(generations_amount) for pokemon_info_raw in pokemons_info_raw]

        generations_info_generator = executor.map(__get_generation_info_from_url, generations_urls, repeat(MAX_QUERIES_PER_SECOND))

    generations_info  = [generation_info_el for generation_info_el in generations_info_generator]

    return FullInfo(pokemons_info, generations_info)


if __name__ == '__main__':
    url = "https://pokeapi.co/api/v2/"
    copy_info_from_api(url)
