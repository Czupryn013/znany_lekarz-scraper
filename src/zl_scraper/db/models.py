"""SQLAlchemy declarative models for the ZnanyLekarz scraping pipeline."""

from datetime import datetime

from sqlalchemy import (
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

    # Company enrichment fields
    website_domain = Column(String(255), nullable=True)
    linkedin_url = Column(String(512), nullable=True)
    domain_searched_at = Column(DateTime, nullable=True)
    linkedin_searched_at = Column(DateTime, nullable=True)

    locations = relationship("ClinicLocation", back_populates="clinic", cascade="all, delete-orphan")
    search_queries = relationship("SearchQuery", back_populates="clinic", cascade="all, delete-orphan")
    doctors = relationship("Doctor", secondary=clinic_doctors, back_populates="clinics")
    linkedin_candidates = relationship("LinkedInCandidate", back_populates="clinic", cascade="all, delete-orphan")

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
