import asyncio
import time

from aiohttp import ClientSession
from progressbar import progressbar
from typing import Dict, List, Optional, Tuple, Union
from utility import retry_on_exception


class Voter(object):
    """ Hold the basic information we have for each voter. """

    def __init__(self, entry):
        split_entry = entry.split(',')
        if len(split_entry) == 4:
            self.first, self.last, self.birth_year, self.zip = split_entry
            self.birth_month = None
        elif len(split_entry) == 5:
            self.first, self.last, self.birth_month, self.birth_year, self.zip = split_entry
            if self.birth_month == 'Unknown':
                self.birth_month = None
        else:
            raise ValueError("String used in voter constructor must contain 4 or 5 commas.")

        self.birth_year = int(self.birth_year)
        if self.birth_month:
            self.birth_month = int(self.birth_month)

    def __hash__(self):
        return hash(' - '.join([self.first, self.last, str(self.birth_year), self.zip]))

    def __str__(self):
        return f"{self.first},{self.last},{self.birth_month if self.birth_month else 'Unknown'},{self.birth_year},{self.zip}"

    def get_api_args(self, month: int) -> Dict:
        """ Get the arguments to be passed into the API calls.
        :param month: Int between 1 and 12 for the month the person was born in.
        :return: Dict of created attributes.
        """
        return {
            "FirstName": self.first,
            "LastName": self.last,
            "NameBirthMonth": month,
            "NameBirthYear": self.birth_year,
            "ZipCode": self.zip,
            # Arguments included in the browser's request, kept here for safety but their role is unclear.
            'Dln': '',
            'DlnBirthMonth': '0',
            'DlnBirthYear': '',
            'DpaID': '0',
            'Months': '',
            'VoterNotFound': 'false',
            'TransistionVoter': 'false'
        }


class RegistrationAPIExecutor(object):
    """ Collection of functions to execute calls against the Michigan Voter Registration API """

    def api_multi_call(self, url: str, arguments: List[Dict]) -> List[Union[Tuple[str, Dict], Tuple[None, None]]]:
        """ Quickly execute multiple API calls on the designated urls, posting the designated arguments """

        async def interior(func, arguments):
            sem = asyncio.Semaphore(50)
            tasks = []

            async with ClientSession() as session:
                for arg in arguments:
                    task = asyncio.ensure_future(func(sem, url, arg, session))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                return responses

        resp = []

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(interior(self._api_call, arguments))
        resp += loop.run_until_complete(future)
        return resp

    def get_voters_with_absentee_ballots(self, voters: List[Voter]) -> List[Voter]:
        """
        For a list of voters, get the ones who have requested absentee ballots, and return the arguments passed
        for the voters who did have absentee ballots.
        """
        registration_url = 'https://mvic.sos.state.mi.us/Voter/SearchByName'
        registered_voters = set()
        voters_with_ballots = set()
        for month in progressbar(range(1, 13)):  # We don't know the birth months for registers so we need to try all
            args = [voter.get_api_args(month) for voter in voters if
                    not voter.birth_month or voter.birth_month == month]
            responses = self.api_multi_call(registration_url, args)
            for voter, response_page in zip(voters, responses):
                if response_page and 'Yes, you are registered!' in response_page:
                    registered_voters.add(voter)
                    if 'Your clerk has not recorded receiving your AV Application.' not in response_page:
                        voter.birth_month = month
                        voters_with_ballots.add(voter)
            # Remove voters who we found the right month for from future API calls.
            voters = [v for v in voters if v not in registered_voters]
        return list(voters_with_ballots)

    @staticmethod
    async def _api_call(sem: asyncio.Semaphore, url: str, argument: Dict, session: ClientSession) -> Optional[str]:
        for attempt_no in range(5):  # Try a max of 5 times.
            try:
                async with sem:
                    async with session.post(url, data=argument) as resp:
                        r = await resp.text()
                        return r
            except Exception as e:
                print(f"Attempt {attempt_no} failed for url {url} with arguments {argument}: {e}.")
                time.sleep(5)
                pass
        print(f"Unable to get response for url {url} with arguments {argument}")
        return None


class RegistrationChecker(object):

    def get_voters_with_ballots(self, in_path='older_registered_voters.txt', out_path='voters_with_absentee_ballots_test.txt'):
        """
        Execute the API calls to get the absentee ballot status for the voters in the incoming file, writing out
        the file of voters (with determined months of birth)
        """
        with open(in_path, 'r') as infile:
            rows = infile.read().strip().split('\n')[2:-1]

        voters = [Voter(row) for row in rows]
        api_exec = RegistrationAPIExecutor()
        voters_with_absentee_ballots = api_exec.get_voters_with_absentee_ballots(voters)
        voters_with_absentee_ballots.sort(key=lambda v: v.birth_year)
        with open(out_path, 'w') as outfile:
            outfile.write('\n'.join([str(v) for v in voters_with_absentee_ballots]))


if __name__ == '__main__':
    s = RegistrationChecker()
    s.get_voters_with_ballots()
