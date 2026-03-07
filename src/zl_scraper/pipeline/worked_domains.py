"""Maintained list of domains to exclude from ICP — demos, lost deals, pipeline, big chains."""

from typing import TypedDict


class WorkedDomain(TypedDict):
    domain: str
    name: str
    reason: str


# fmt: off
WORKED_DOMAINS: list[WorkedDomain] = [
    # ── Big chains ────────────────────────────────────────────────────────
    {"domain": "medicover.pl","name": "Medicover","reason": "big chain"},
    {"domain": "luxmed.pl","name": "LUX MED","reason": "big chain"},
    {"domain": "enel.pl","name": "enel-med","reason": "big chain"},
    {"domain": "medicover.com","name": "Medicover (intl)","reason": "big chain"},
    {"domain": "polmed.pl","name": "Polmed","reason": "big chain"},
    {"domain": "centermed.pl","name": "CenterMed","reason": "big chain"},
    # ── Customer ────────────────────────────────────────────────────────
    {"domain": "openmed.pl", "name": "OpenMed", "reason": "customer"},
    {"domain": "dcg.com.pl", "name": "DCH", "reason": "customer"},
    {"domain": "macromedica.pl", "name": "MacroMedica", "reason": "customer"},
    {"domain": "multiclinic.pl", "name": "MultiClinic", "reason": "customer"},
    {"domain": "wrocm.pl", "name": "WroCM", "reason": "customer"},
    {"domain": "progamed.pl", "name": "Progamed", "reason": "customer"},
    {"domain": "centrum-psych.pl", "name": "CZiP", "reason": "customer"},
    {"domain": "inter-med.pl", "name": "InterMed", "reason": "customer"},
    {"domain": "goodlifeclinic.pl", "name": "Good Life Clinic", "reason": "customer"},
    {"domain": "medissmedical.pl", "name": "Mediss Medical", "reason": "customer"},
    {"domain": "brodnicapsycholog.pl", "name": "Danmedic", "reason": "customer"},
    {"domain": "omegamc.pl", "name": "Omega MC Bydgoszcz", "reason": "customer"},
    {"domain": "omegamc-wroclaw.pl", "name": "Omega MC Wro", "reason": "customer"},
    {"domain": "ginemedica.pl", "name": "Ginemedica", "reason": "customer"},
    {"domain": "nzozppl.pl", "name": "Henryk Domański", "reason": "customer"},
    {"domain": "angelius.pl", "name": "Angelius", "reason": "customer"},
    {"domain": "esteklinika.pl", "name": "EsteKlinika (Angelius)", "reason": "customer"},
    # ── Other ────────────────────────────────────────────────────────────
    {"domain": "ocho.pl", "name": "Grupa OCHO", "reason": "już to robią u siebie"},
    {"domain": "medispace.pl", "name": "MediSpace", "reason": "na razie nie chcą"},
    {"domain": "spls.pl", "name": "Przychodnia Śródmieście", "reason": "problemy z rodo"},
    {"domain": "mokotooth.com", "name": "Mokotooth", "reason": "nie mają zgód rodo"},
    {"domain": "vita.legnica.pl", "name": "Vita Plus Legnica", "reason": "nie mają potrzeby opinii, i tak mają pacjentów"},
    {"domain": "cdk24.pl", "name": "CDK 24", "reason": "owner ghostował a potem podziękował"},
    {"domain": "centrum.med.pl", "name": "Centrum Med-Diag", "reason": "nie jest to teraz ważne"},
    {"domain": "goldenmed.pl", "name": "GoldenMed", "reason": "już to robią u siebie"},
    {"domain": "promedicalclinic.pl", "name": "Pro Medical", "reason": "obawy o rodo mają i co chwila zmieniali decyzję"},
    {"domain": "rehab-gdansk.pl", "name": "REHAB Gdańsk", "reason": "fizjo, za maili i średnio to im potrzebne"},
    {"domain": "cm-vivamedica.pl", "name": "VivaMedica Wrocław", "reason": "za 50 zł chciał xd"},
    {"domain": "Artmedik24.pl", "name": "Artmedik", "reason": "nie potrzebują opinii na nfz"},
    {"domain": "PozCero.pl", "name": "PozCero", "reason": "nie jest to dla nich zbyt pilne"},
    {"domain": "carolina.pl", "name": "Szpital Carolina", "reason": "za duży risk z RODO dla nich"},
    {"domain": "zdrowieodnowa.com.pl", "name": "Zdrowie Odnowa", "reason": "za małe, opinie nie są tak ważne"},
    {"domain": "medykdlaciebie.pl", "name": "Medyk Dla Ciebie", "reason": "sami próbują zbierać na recepcji"},
    {"domain": "amicare.pl", "name": "AmiCare", "reason": "mało wizyt, opinie średnio ważne"},
    {"domain": "omedica.pl", "name": "Omedica", "reason": "owner mnie ignoruje"},
    {"domain": "cmp.med.pl", "name": "CMP", "reason": "duża sieć, obecnie sami próbują zbierać nie chcą pomocy"},
    {"domain": "Dermoklinika.pl", "name": "Dermoklinika", "reason": "nie jest to dla nich ważne i nie widzą wartości w opiniach"},
    {"domain": "Vilamed.pl", "name": "Vilamed", "reason": "nie jest to priorytet"},
    {"domain": "cityclinic.pl", "name": "City Clinic", "reason": "mało hajsu mają i nie chca wydawać"},
    {"domain": "veritamed.com", "name": "VeritaMed", "reason": "nie chcą udostępniać danych"},
    {"domain": "polmed.pl", "name": "Polmed", "reason": "duża sieć, już coś wdrożyli u siebie"},
    {"domain": "kardiosystem.pl", "name": "KardioSystem", "reason": "chętnei marzec-kwiecień"},
    {"domain": "PhysioTeam.pl", "name": "PhysioTeam", "reason": "niby chętnie, ale dop maj"},
    {"domain": "gizinscy.pl", "name": "CM Gizińscy", "reason": "niby chętnie, koniec marca"},
    {"domain": "Kriosonik.pl", "name": "Kriosonik", "reason": "postoponed na koniec marca"},
    {"domain": "SanVitas.pl", "name": "SanVitas", "reason": "na kwiecień odłożył temat"},
    {"domain": "szpitaleskulap.pl", "name": "Szpital Eskulap", "reason": "closing"},
    {"domain": "popularnacm.pl", "name": "CM Popularna", "reason": "closing (chyba)"},
    {"domain": "Fimedica.pl", "name": "Fimedica", "reason": "closing"},
    {"domain": "UniCardia.pl", "name": "UniCardia", "reason": "closing"},
    {"domain": "poznansieusmiecha.pl", "name": "Partner Dent", "reason": "chętny, a potem ghostuje"},
    {"domain": "Ottobock.pl", "name": "Ottobock", "reason": "nie icp, nie wiadomo jaka decyzja"},
    {"domain": "pentahospitals.pl", "name": "Penta Hospitals", "reason": "niby chętni, ale muszą przetestować demo"},
    {"domain": "vita-medica.pl", "name": "Vita Medica", "reason": "no decision od dyr zarządzającej"},
    {"domain": "gzmedica.pl", "name": "GZ Medica", "reason": "czeka na decyzję"},
    {"domain": "superiorcm.pl", "name": "Superior CM", "reason": "czeka na decyzję"},
    {"domain": "cm-klara.pl", "name": "CM klara", "reason": "czeka na decyzję"},
    {"domain": "unicare.pl", "name": "Unicare", "reason": "niby chętny, ale musi z żoną potwierdzić"},
    {"domain": "klinikanova.pl", "name": "Klinika Nova", "reason": "umówiony był call z ownerem ale called off"},
    {"domain": "multimed-bierun.pl", "name": "MultiMed Bieruń", "reason": "sam umówił calla"},
    {"domain": "guardianclinic.pl", "name": "Guardian Clinic", "reason": "call umówiony"},
]
# fmt: on


def get_worked_domain_set() -> set[str]:
    """Return a lowercase set of all worked domains for fast lookup."""
    return {entry["domain"].lower() for entry in WORKED_DOMAINS}
