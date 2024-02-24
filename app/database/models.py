from typing import List, Optional

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy import String, Integer, ForeignKey, Column, Table
from sqlalchemy.schema import MetaData


class Base(AsyncAttrs, DeclarativeBase):
    pass


post_tags = Table(
    "post_tags",
    Base.metadata,
    Column("post_id", ForeignKey("question_posts.id"), primary_key=True),
    Column("tag_id", ForeignKey("tags.id"), primary_key=True),
)


class Tag(Base):
    __tablename__ = "tags"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    count_usage: Mapped[int]


class QuestionPost(Base):
    __tablename__ = "question_posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    start: Mapped[int]
    length: Mapped[int]
    score: Mapped[int]
    accepted_answer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("answer_posts.id"))
    accepted_answer: Mapped[Optional["AnswerPost"]] = relationship(back_populates="accepted_answer_id",
                                                                   foreign_keys="answer_posts.id")
    answer_posts: Mapped[List["AnswerPost"]] = relationship(back_populates="question_post",
                                                            foreign_keys="answer_posts.question_post_id")
    tags: Mapped[List[Tag]] = relationship(
        secondary=post_tags
    )


class AnswerPost(Base):
    __tablename__ = "answer_posts"
    id: Mapped[int] = mapped_column(primary_key=True)
    start: Mapped[int]
    length: Mapped[int]
    score: Mapped[int]
    question_post_id: Mapped[int] = mapped_column(ForeignKey("question_posts.id"))
    question_post: Mapped["QuestionPost"] = relationship(back_populates="answer_posts")


class ConfigValues(Base):
    __tablename__ = "configs"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(unique=True)
    value: Mapped[bytes]
# from tortoise.models import Model
# from tortoise import fields
#
#
# class Post(Model):
#     """Posts"""
#     id = fields.BigIntField(pk=True)
#
#     solved = fields.BooleanField()
#
#     start_bytes = fields.BigIntField()
#     length = fields.IntField()
#
#     accepted_answer_id = fields.BigIntField(unique=True)
#     accepted_answer: fields.ReverseRelation["PostAnswer"]
#
#     posts: fields.ReverseRelation["PostAnswer"]
#     tags: fields.ManyToManyRelation["Tag"] = fields.ManyToManyField(
#         "models.Tag", related_name="posts", through="tag_to_posts"
#     )
#
#
# class PostAnswer(Model):
#     """Answers for posts"""
#     id = fields.BigIntField(pk=True)
#     post_id = fields.BigIntField()
#     start_bytes = fields.BigIntField()
#     length = fields.IntField()
#
#     question_post: fields.ForeignKeyRelation[Post] = fields.ForeignKeyField(
#         "models.Post", related_name="posts"
#     )
#
#     accepted_post: fields.ForeignKeyRelation[Post] = fields.ForeignKeyField(
#         "models.Post", related_name="accepted_answer", to_field="accepted_answer_id"
#     )
#
#
# class Tag(Model):
#     """Tag for posts"""
#     id = fields.BigIntField(pk=True)
#     name = fields.CharField(unique=True, max_length=255)
#     count_usage = fields.IntField()
#     posts: fields.ManyToManyRelation[Post]
