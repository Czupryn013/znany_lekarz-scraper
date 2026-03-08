"""SQLAlchemy declarative models for the ZnanyLekarz scraping pipeline."""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# M2M association table: clinics <-> doctors
clinic_doctors = Table(
    "clinic_doctors",
    Base.metadata,
    Column("clinic_id", Integer, ForeignKey("clinics.id", ondelete="CASCADE"), primary_key=True),
    Column("doctor_id", Integer, ForeignKey("doctors.id", ondelete="CASCADE"), primary_key=True),
)

# M2M association table: leads <-> clinics (with role)
lead_clinic_roles = Table(
    "lead_clinic_roles",
    Base.metadata,
    Column("lead_id", Integer, ForeignKey("leads.id", ondelete="CASCADE"), primary_key=True),
    Column("clinic_id", Integer, ForeignKey("clinics.id", ondelete="CASCADE"), primary_key=True),
    Column("role", String(128), nullable=False, primary_key=True),
)


class Specialization(Base):
    """Medical specialization from ZnanyLekarz (e.g. ginekolog, ortopeda)."""

    __tablename__ = "specializations"

    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(255), nullable=False)

    search_queries = relationship("SearchQuery", back_populates="specialization")
    scrape_progress = relationship("ScrapeProgress", back_populates="specialization", uselist=False)

    def __repr__(self) -> str:
        return f"<Specialization(id={self.id}, name='{self.name}')>"


class Clinic(Base):
    """A clinic discovered from ZnanyLekarz search results."""

    __tablename__ = "clinics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    zl_url = Column(String(512), unique=True, nullable=False, index=True)
    name = Column(String(512), nullable=True)
    zl_profile_id = Column(String(64), nullable=True)
    nip = Column(String(32), nullable=True)
    legal_name = Column(String(512), nullable=True)
    description = Column(Text, nullable=True)
    zl_reviews_cnt = Column(Integer, nullable=True)
    doctors_count = Column(Integer, nullable=True)
    discovered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    enriched_at = Column(DateTime, nullable=True)

    # ICP filter flag
    icp_match = Column(Boolean, nullable=False, default=False)

    # Company enrichment fields
    website_domain = Column(String(255), nullable=True)
    linkedin_url = Column(String(512), nullable=True)
    domain_searched_at = Column(DateTime, nullable=True)
    linkedin_searched_at = Column(DateTime, nullable=True)
    nip_searched_at = Column(DateTime, nullable=True)

    # KRS / CEIDG registry fields
    krs_number = Column(String(32), nullable=True)
    regon = Column(String(32), nullable=True)
    registration_date = Column(String(32), nullable=True)
    legal_type = Column(String(16), nullable=True)  # KRS, CEIDG_JDG, CEIDG_SC, NOT_FOUND
    krs_searched_at = Column(DateTime, nullable=True)

    locations = relationship("ClinicLocation", back_populates="clinic", cascade="all, delete-orphan")
    search_queries = relationship("SearchQuery", back_populates="clinic", cascade="all, delete-orphan")
    doctors = relationship("Doctor", secondary=clinic_doctors, back_populates="clinics")
    linkedin_candidates = relationship("LinkedInCandidate", back_populates="clinic", cascade="all, delete-orphan")
    board_members = relationship("BoardMember", back_populates="clinic", cascade="all, delete-orphan")
    leads = relationship("Lead", secondary=lead_clinic_roles, back_populates="clinics")

    def __repr__(self) -> str:
        return f"<Clinic(id={self.id}, name='{self.name}', zl_url='{self.zl_url}')>"


class ClinicLocation(Base):
    """A physical location (address + coordinates) belonging to a clinic."""

    __tablename__ = "clinic_locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    clinic_id = Column(Integer, ForeignKey("clinics.id", ondelete="CASCADE"), nullable=False)
    address = Column(String(512), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    facebook_url = Column(String(512), nullable=True)
    instagram_url = Column(String(512), nullable=True)
    youtube_url = Column(String(512), nullable=True)
    linkedin_url = Column(String(512), nullable=True)
    website_url = Column(String(512), nullable=True)

    clinic = relationship("Clinic", back_populates="locations")

    def __repr__(self) -> str:
        return f"<ClinicLocation(id={self.id}, address='{self.address}')>"


class Doctor(Base):
    """A doctor listed on a clinic's profile page (M2M with clinics)."""

    __tablename__ = "doctors"

    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(256), nullable=True)
    surname = Column(String(256), nullable=True)
    zl_url = Column(String(512), nullable=True)

    clinics = relationship("Clinic", secondary=clinic_doctors, back_populates="doctors")

    def __repr__(self) -> str:
        return f"<Doctor(id={self.id}, name='{self.name} {self.surname}')>"


class SearchQuery(Base):
    """Links a clinic to the specialization search that discovered it."""

    __tablename__ = "search_queries"
    __table_args__ = (
        UniqueConstraint("clinic_id", "specialization_id", name="uq_clinic_specialization"),
    )

    clinic_id = Column(Integer, ForeignKey("clinics.id", ondelete="CASCADE"), primary_key=True)
    specialization_id = Column(Integer, ForeignKey("specializations.id", ondelete="CASCADE"), primary_key=True)
    discovered_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    clinic = relationship("Clinic", back_populates="search_queries")
    specialization = relationship("Specialization", back_populates="search_queries")

    def __repr__(self) -> str:
        return f"<SearchQuery(clinic_id={self.clinic_id}, spec_id={self.specialization_id})>"


class ScrapeProgress(Base):
    """Checkpoint/resume tracker per specialization during discovery."""

    __tablename__ = "scrape_progress"

    specialization_id = Column(
        Integer,
        ForeignKey("specializations.id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_page_scraped = Column(Integer, nullable=False, default=0)
    total_pages = Column(Integer, nullable=True)
    status = Column(String(32), nullable=False, default="pending")  # pending / in_progress / done
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    specialization = relationship("Specialization", back_populates="scrape_progress")

    def __repr__(self) -> str:
        return (
            f"<ScrapeProgress(spec_id={self.specialization_id}, "
            f"page={self.last_page_scraped}, status='{self.status}')>"
        )


class BoardMember(Base):
    """A board member / prokura / owner discovered from KRS or CEIDG registry."""

    __tablename__ = "board_members"

    id = Column(Integer, primary_key=True, autoincrement=True)
    clinic_id = Column(Integer, ForeignKey("clinics.id", ondelete="CASCADE"), nullable=False)
    full_name = Column(String(256), nullable=False)
    pesel = Column(String(11), nullable=True)
    role = Column(String(128), nullable=True)
    source = Column(String(16), nullable=False)  # KRS_BOARD, KRS_PROKURA, CEIDG_JDG, CEIDG_SC

    clinic = relationship("Clinic", back_populates="board_members")

    def __repr__(self) -> str:
        return f"<BoardMember(id={self.id}, name='{self.full_name}', source='{self.source}')>"


class Lead(Base):
    """A person (board member / owner / employee) tracked for phone enrichment."""

    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pesel = Column(String(11), nullable=True, unique=True, index=True)
    full_name = Column(String(256), nullable=False)
    phone = Column(String(64), nullable=True)
    email = Column(String(256), nullable=True)
    linkedin_url = Column(String(512), nullable=True)
    lead_source = Column(String(32), nullable=False)  # KRS, JDG, SC, LINKEDIN, EMPLOYEE
    phone_source = Column(String(32), nullable=True)  # PROSPEO, FULLENRICH, LUSHA, CEIDG
    enrichment_status = Column(String(32), nullable=False, default="PENDING")  # see EnrichmentStatus
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=True, onupdate=datetime.utcnow)

    clinics = relationship("Clinic", secondary=lead_clinic_roles, back_populates="leads")

    def __repr__(self) -> str:
        return (
            f"<Lead(id={self.id}, name='{self.full_name}', "
            f"status='{self.enrichment_status}', phone='{self.phone}')>"
        )


class LinkedInCandidate(Base):
    """A LinkedIn company page URL found via SERP, pending human review."""

    __tablename__ = "linkedin_candidates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    clinic_id = Column(Integer, ForeignKey("clinics.id", ondelete="CASCADE"), nullable=False)
    url = Column(String(512), nullable=False)
    status = Column(String(16), nullable=False, default="maybe")  # yes / maybe / no
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    clinic = relationship("Clinic", back_populates="linkedin_candidates")

    def __repr__(self) -> str:
        return f"<LinkedInCandidate(id={self.id}, clinic_id={self.clinic_id}, status='{self.status}')>"
